from django.core.management.base import BaseCommand
from django.utils import timezone
from games.igdb_api import make_igdb_request
from games.models import Game
import time


class BaseIgdbCommand(BaseCommand):
    """Базовый класс для команд загрузки IGDB"""

    def add_arguments(self, parser):
        """Общие аргументы для всех команд"""
        parser.add_argument('--tactical-rpg', action='store_true',
                            help='Загружать только тактические RPG')
        parser.add_argument('--overwrite', action='store_true',
                            help='Удалить существующие игры и загрузить заново')
        parser.add_argument('--debug', action='store_true',
                            help='Включить режим отладки')
        parser.add_argument('--limit', type=int, default=0,
                            help='Ограничить количество загружаемых игр (0 - без ограничения)')
        parser.add_argument('--offset', type=int, default=0,
                            help='Пропустить указанное количество игр (пагинация)')
        parser.add_argument('--min-rating-count', type=int, default=0,
                            help='Минимальное количество оценок для фильтрации (0 - без фильтра)')
        parser.add_argument('--skip-existing', action='store_true',
                            help='Пропускать игры, которые уже есть в базе данных')

    def handle(self, *args, **options):
        """Основной метод выполнения команды"""
        tactical_rpg = options['tactical_rpg']
        overwrite = options['overwrite']
        debug = options['debug']
        limit = options['limit']
        offset = options['offset']
        min_rating_count = options['min_rating_count']
        skip_existing = options['skip_existing']

        self.stdout.write('🎮 ЗАГРУЗКА ИГР ИЗ IGDB')
        self.stdout.write('=' * 60)

        # Определяем тип загрузки
        if tactical_rpg:
            self.stdout.write('🎯 РЕЖИМ: Тактические RPG')
        else:
            self.stdout.write('📊 РЕЖИМ: Все популярные игры')

        if limit > 0:
            self.stdout.write(f'📊 ЛИМИТ: загружается не более {limit} игр')
        if offset > 0:
            self.stdout.write(f'⏭️  OFFSET: пропускаем первые {offset} игр')
        if min_rating_count > 0:
            self.stdout.write(f'⭐ ФИЛЬТР: игры с не менее {min_rating_count} оценками')
        if skip_existing:
            self.stdout.write('⏭️  SKIP-EXISTING: пропуск игр, которые уже есть в базе')
        if overwrite:
            self.stdout.write('🔄 OVERWRITE: найденные игры будут удалены и загружены заново')

        if debug:
            self.stdout.write('🐛 РЕЖИМ ОТЛАДКИ ВКЛЮЧЕН')
            self.stdout.write('-' * 40)

        # Проверяем совместимость опций
        if overwrite and skip_existing:
            self.stdout.write('⚠️  ВНИМАНИЕ: опция --skip-existing игнорируется, так как включен --overwrite')
            skip_existing = False

        # Загружаем игры в зависимости от режима
        from .tactical_rpg_loader import TacticalRpgLoader
        tactical_loader = TacticalRpgLoader(self.stdout, self.stderr)

        if tactical_rpg:
            all_games = tactical_loader.load_tactical_rpg_games(
                debug, limit, offset, min_rating_count, skip_existing
            )
        else:
            all_games = self.load_all_popular_games(
                debug, limit, offset, min_rating_count, skip_existing
            )

        if not all_games:
            self.stdout.write('❌ Не найдено игр для загрузки')
            return

        self.stdout.write(f'📥 Найдено игр для обработки: {len(all_games)}')

        # Обработка режима перезаписи
        if overwrite:
            self._handle_overwrite_mode(all_games, debug)

        # Обработка данных
        from .data_collector import DataCollector
        collector = DataCollector(self.stdout, self.stderr)

        result_stats = collector.process_all_data_sequentially(all_games, debug)

        # КРАТКАЯ статистика в конце (если не в режиме отладки)
        if not debug:
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА!')
            self.stdout.write(f'⏱️  Время: {result_stats["total_time"]:.2f}с')

            if tactical_rpg:
                self.stdout.write('🎯 Тип: Тактические RPG')
            else:
                self.stdout.write('📊 Тип: Все популярные игры')

            if limit > 0:
                self.stdout.write(f'📊 Лимит: {limit}')
            if offset > 0:
                self.stdout.write(f'⏭️  Offset: {offset}')
            if min_rating_count > 0:
                self.stdout.write(f'⭐ Мин. оценок: {min_rating_count}')

            self.stdout.write(f'🎮 Найдено: {result_stats["total_games_found"]}')
            self.stdout.write(f'✅ Загружено: {result_stats["created_count"]}')
            self.stdout.write(f'⏭️  Пропущено: {result_stats["skipped_count"]}')

    def _handle_overwrite_mode(self, all_games, debug):
        """Обрабатывает режим перезаписи"""
        self.stdout.write('🔄 РЕЖИМ ПЕРЕЗАПИСИ - найденные игры будут удалены и загружены заново!')

        # Получаем ID найденных игр
        game_ids_to_delete = [game_data.get('id') for game_data in all_games if game_data.get('id')]

        if game_ids_to_delete:
            if debug:
                self.stdout.write(f'   🔍 Поиск игр для удаления: {len(game_ids_to_delete)} ID')

            # Находим игры в базе по igdb_id
            games_to_delete = Game.objects.filter(igdb_id__in=game_ids_to_delete)
            count_before = games_to_delete.count()

            if debug:
                self.stdout.write(f'   📊 Найдено игр для удаления в базе: {count_before}')

            if count_before > 0:
                # Удаляем найденные игры (связанные объекты удалятся каскадно)
                deleted_info = games_to_delete.delete()

                # Разбираем результат delete()
                if isinstance(deleted_info, tuple) and len(deleted_info) == 2:
                    total_deleted, deleted_details = deleted_info

                    # Выводим детализированную статистику
                    self.stdout.write(f'🗑️  УДАЛЕНИЕ ЗАВЕРШЕНО:')
                    self.stdout.write(f'   • Всего удалено объектов: {total_deleted}')

                    # Выводим детали по моделям
                    for model_name, count in deleted_details.items():
                        model_display = model_name.split('.')[-1]  # Извлекаем имя модели
                        if count > 0:
                            self.stdout.write(f'   • {model_display}: {count}')
                else:
                    # Для старых версий Django
                    self.stdout.write(f'🗑️  Удалено игр и связанных данных: {deleted_info}')
            else:
                self.stdout.write('   ℹ️  Не найдено игр для удаления в базе данных')
        else:
            self.stdout.write('   ⚠️  Не найдено ID игр для удаления')

    def load_all_popular_games(self, debug=False, limit=0, offset=0, min_rating_count=0, skip_existing=False):
        """Загрузка всех игр с сортировкой по популярности (rating_count)"""
        from .data_collector import DataCollector
        collector = DataCollector(self.stdout, self.stderr)
        return collector.load_all_popular_games(debug, limit, offset, min_rating_count, skip_existing)

    def create_game_object(self, game_data, cover_map):
        """Создает объект игры"""
        game = Game(
            igdb_id=game_data.get('id'),
            name=game_data.get('name', ''),
            summary=game_data.get('summary', ''),
            storyline=game_data.get('storyline', ''),
            rating=game_data.get('rating'),
            rating_count=game_data.get('rating_count', 0)
        )

        if game_data.get('first_release_date'):
            from datetime import datetime
            naive_datetime = datetime.fromtimestamp(game_data['first_release_date'])
            game.first_release_date = timezone.make_aware(naive_datetime)

        cover_id = game_data.get('cover')
        if cover_id and cover_id in cover_map:
            game.cover_url = cover_map[cover_id]

        return game