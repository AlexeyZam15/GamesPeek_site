# games/management/commands/find_game_offset.py
from django.core.management.base import BaseCommand
from django.db.models import Q
from games.models import Game
import math


class Command(BaseCommand):
    """Команда для определения позиции игры в базе данных по названию"""

    help = 'Находит позицию игры в базе данных для использования в offset'

    def add_arguments(self, parser):
        parser.add_argument('game_name', type=str,
                            help='Название игры для поиска (можно часть названия)')
        parser.add_argument('--exact', action='store_true',
                            help='Точный поиск (по умолчанию частичное совпадение)')
        parser.add_argument('--order-by', type=str, default='id',
                            choices=['id', 'name', 'rating', '-id', '-name', '-rating'],
                            help='Поле для сортировки (по умолчанию id)')
        parser.add_argument('--verbose', action='store_true',
                            help='Показать детальную информацию об игре')
        parser.add_argument('--page-size', type=int, default=100,
                            help='Размер страницы для расчета (по умолчанию 100)')

    def handle(self, *args, **options):
        game_name = options['game_name']
        exact = options.get('exact', False)
        order_by = options.get('order_by', 'id')
        verbose = options.get('verbose', False)
        page_size = options.get('page_size', 100)

        self.stdout.write('\n' + '=' * 70)
        self.stdout.write(f'🔍 ПОИСК ПОЗИЦИИ ИГРЫ: "{game_name}"')
        self.stdout.write('=' * 70)

        # Поиск игры
        if exact:
            games = Game.objects.filter(name__iexact=game_name)
        else:
            games = Game.objects.filter(name__icontains=game_name)

        total_found = games.count()

        if total_found == 0:
            self.stdout.write(self.style.ERROR(f'❌ Игра "{game_name}" не найдена в базе данных'))
            return

        if total_found > 1:
            self.stdout.write(self.style.WARNING(f'⚠️ Найдено несколько игр ({total_found}):'))

            # Показываем первые 20 для выбора
            for i, game in enumerate(games[:20], 1):
                self.stdout.write(f'   {i}. {game.name} (ID: {game.igdb_id})')

            if total_found > 20:
                self.stdout.write(f'   ... и еще {total_found - 20}')

            self.stdout.write('\n💡 Используйте более точное название или --exact для точного поиска')

            # Спрашиваем, какую игру выбрать
            if total_found <= 20:
                choice = input('\nВведите номер игры для детального просмотра (или Enter для выхода): ').strip()
                if choice and choice.isdigit():
                    choice_idx = int(choice) - 1
                    if 0 <= choice_idx < total_found:
                        game = games[choice_idx]
                        self._show_game_position(game, order_by, page_size, verbose)
            return

        # Найдена одна игра
        game = games.first()
        self._show_game_position(game, order_by, page_size, verbose)

    def _show_game_position(self, game, order_by, page_size, verbose):
        """Показывает позицию игры в отсортированном списке"""

        self.stdout.write(f'\n🎮 НАЙДЕНА ИГРА:')
        self.stdout.write(f'   • Название: {game.name}')
        self.stdout.write(f'   • IGDB ID: {game.igdb_id}')
        self.stdout.write(f'   • ID в базе: {game.id}')

        if verbose:
            self.stdout.write(f'   • Рейтинг: {game.rating if game.rating else "Нет"}')
            self.stdout.write(f'   • Дата релиза: {game.first_release_date if game.first_release_date else "Нет"}')
            self.stdout.write(f'   • Обложка: {"✅ Есть" if game.cover_url else "❌ Нет"}')
            self.stdout.write(f'   • Описание: {"✅ Есть" if game.summary and game.summary.strip() else "❌ Нет"}')
            self.stdout.write(f'   • Жанры: {game.genres.count()}')
            self.stdout.write(f'   • Платформы: {game.platforms.count()}')
            self.stdout.write(f'   • Скриншоты: {game.screenshots.count()}')

        self.stdout.write('\n' + '=' * 70)
        self.stdout.write('📊 ПОЗИЦИЯ В БАЗЕ ДАННЫХ')
        self.stdout.write('=' * 70)

        # Общее количество игр
        total_games = Game.objects.count()
        self.stdout.write(f'📦 Всего игр в базе: {total_games}')

        # Определяем поле для сортировки и направление
        order_desc = order_by.startswith('-')
        order_field = order_by.lstrip('-')

        order_display = {
            'id': 'ID (по умолчанию)',
            'name': 'названию',
            'rating': 'рейтингу'
        }.get(order_field, order_field)

        direction = 'по убыванию' if order_desc else 'по возрастанию'
        self.stdout.write(f'🔽 Сортировка по {order_display} {direction}')

        # Создаем запрос для подсчета позиции
        if order_desc:
            # По убыванию
            if order_field == 'id':
                position = Game.objects.filter(id__gt=game.id).count()
            elif order_field == 'name':
                position = Game.objects.filter(
                    Q(name__gt=game.name) |
                    Q(name=game.name, id__gt=game.id)
                ).count()
            elif order_field == 'rating':
                position = Game.objects.filter(
                    Q(rating__gt=game.rating) |
                    Q(rating=game.rating, id__gt=game.id)
                ).count()
            else:
                position = 0
        else:
            # По возрастанию
            if order_field == 'id':
                position = Game.objects.filter(id__lt=game.id).count()
            elif order_field == 'name':
                position = Game.objects.filter(
                    Q(name__lt=game.name) |
                    Q(name=game.name, id__lt=game.id)
                ).count()
            elif order_field == 'rating':
                position = Game.objects.filter(
                    Q(rating__lt=game.rating) |
                    Q(rating=game.rating, id__lt=game.id)
                ).count()
            else:
                position = 0

        # Позиция (0-based или 1-based?)
        position_0based = position
        position_1based = position + 1

        self.stdout.write(f'\n📍 ПОЗИЦИЯ ИГРЫ:')
        self.stdout.write(f'   • 0-based позиция (для offset): {position_0based}')
        self.stdout.write(f'   • 1-based позиция: {position_1based} из {total_games}')

        # Проценты
        percentage_before = (position_0based / total_games * 100) if total_games > 0 else 0
        percentage_after = ((total_games - position_1based) / total_games * 100) if total_games > 0 else 0

        self.stdout.write(f'   • Игр до: {position_0based} ({percentage_before:.1f}%)')
        self.stdout.write(f'   • Игр после: {total_games - position_1based} ({percentage_after:.1f}%)')

        # Расчет страниц
        if page_size > 0:
            page_number = position_0based // page_size + 1
            total_pages = math.ceil(total_games / page_size)
            position_on_page = position_0based % page_size + 1

            self.stdout.write(f'\n📄 ИНФОРМАЦИЯ ДЛЯ ПАГИНАЦИИ (размер страницы: {page_size}):')
            self.stdout.write(f'   • Номер страницы: {page_number} из {total_pages}')
            self.stdout.write(f'   • Позиция на странице: {position_on_page}')

        # Рекомендации для команд
        self.stdout.write('\n' + '=' * 70)
        self.stdout.write('🚀 РЕКОМЕНДАЦИИ ДЛЯ ЗАПУСКА КОМАНД')
        self.stdout.write('=' * 70)

        # Для load_games
        self.stdout.write('\n📥 ДЛЯ ЗАГРУЗКИ ИГР (load_games):')

        if position_0based > 0:
            self.stdout.write(f'   python manage.py load_games --offset {position_0based} [другие параметры]')
            self.stdout.write(f'   (начнет загрузку с этой игры)')
        else:
            self.stdout.write(f'   ⚠️ Игра уже в начале списка (offset 0)')

        # Для обновления данных
        self.stdout.write('\n🔄 ДЛЯ ОБНОВЛЕНИЯ ДАННЫХ (update-missing-data):')

        # Вариант 1: начать с этой игры
        self.stdout.write(f'   Вариант 1 (начать с этой игры):')
        self.stdout.write(f'   python manage.py load_games --update-missing-data --offset {position_0based}')

        # Вариант 2: обновить только эту игру
        self.stdout.write(f'\n   Вариант 2 (только эта игра):')
        self.stdout.write(f'   python manage.py load_games --update-missing-data --game-names "{game.name}"')

        # Для обновления обложек
        self.stdout.write('\n🖼️  ДЛЯ ОБНОВЛЕНИЯ ОБЛОЖЕК (update-covers):')
        self.stdout.write(f'   python manage.py load_games --update-covers --offset {position_0based}')

        # Для повторного запуска с сохранением offset
        if position_0based > 0:
            self.stdout.write('\n💾 ДЛЯ ПРОДОЛЖЕНИЯ С СОХРАНЕННЫМ OFFSET:')
            self.stdout.write(f'   Текущий offset для сохранения: {position_0based}')
            self.stdout.write(f'   (offset автоматически сохраняется при использовании --repeat)')

        # Дополнительная информация
        self.stdout.write('\n📌 ПРИМЕРЫ ПОЛНЫХ КОМАНД:')

        # Пример 1: продолжить загрузку популярных игр
        self.stdout.write(f'\n   Пример 1: Продолжить загрузку популярных игр')
        self.stdout.write(f'   python manage.py load_games --offset {position_0based} --repeat 0')

        # Пример 2: продолжить обновление данных
        self.stdout.write(f'\n   Пример 2: Продолжить обновление данных')
        self.stdout.write(f'   python manage.py load_games --update-missing-data --offset {position_0based} --repeat 0')

        self.stdout.write('\n' + '=' * 70)