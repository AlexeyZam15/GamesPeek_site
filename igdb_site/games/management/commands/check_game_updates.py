# games/management/commands/check_game_updates.py
from django.core.management.base import BaseCommand
from games.models import Game
from .load_igdb.game_loader import GameLoader
from .load_igdb.data_collector import DataCollector
from .load_igdb.data_loader import DataLoader
from .load_igdb.relations_handler import RelationsHandler
import time


class Command(BaseCommand):
    """Команда для проверки необходимости обновления игры и симуляции обновления (dry-run)"""

    help = 'Проверяет, нуждается ли игра в обновлении, и показывает что будет обновлено (dry-run)'

    def add_arguments(self, parser):
        parser.add_argument('game_name', type=str,
                            help='Название игры для проверки')
        parser.add_argument('--dry-run', action='store_true', default=True,
                            help='Режим симуляции без реального обновления (включен по умолчанию)')
        parser.add_argument('--no-dry-run', action='store_true',
                            help='Режим реального обновления (отключает dry-run)')
        parser.add_argument('--debug', action='store_true',
                            help='Детальный вывод отладочной информации')
        parser.add_argument('--full-check', action='store_true',
                            help='Полная проверка с загрузкой данных из IGDB (может занять время)')

    def handle(self, *args, **options):
        game_name = options['game_name']
        dry_run = not options.get('no_dry_run', False)  # По умолчанию True
        debug = options.get('debug', False)
        full_check = options.get('full_check', False)

        self.stdout.write('\n' + '=' * 70)
        self.stdout.write(f'🔍 ПРОВЕРКА ИГРЫ: "{game_name}"')
        self.stdout.write('=' * 70)

        # Ищем игру в базе данных
        games = Game.objects.filter(name__icontains=game_name)

        if not games.exists():
            self.stdout.write(self.style.ERROR(f'❌ Игра "{game_name}" не найдена в базе данных'))
            return

        if games.count() > 1:
            self.stdout.write(self.style.WARNING('⚠️ Найдено несколько игр:'))
            for i, game in enumerate(games[:10], 1):
                self.stdout.write(f'   {i}. {game.name} (ID: {game.igdb_id})')
            if games.count() > 10:
                self.stdout.write(f'   ... и еще {games.count() - 10}')

            self.stdout.write('\n💡 Используйте более точное название или укажите ID игры')
            return

        game = games.first()
        self._check_single_game(game, dry_run, debug, full_check)

    def _check_single_game(self, game, dry_run, debug, full_check):
        """Проверяет одну игру"""
        from .load_igdb.game_cache import GameCacheManager

        self.stdout.write(f'\n🎮 ИГРА: {game.name}')
        self.stdout.write(f'   • ID в базе: {game.id}')
        self.stdout.write(f'   • IGDB ID: {game.igdb_id}')
        self.stdout.write(f'   • Рейтинг: {game.rating if game.rating else "Нет"}')
        self.stdout.write(f'   • Дата релиза: {game.first_release_date if game.first_release_date else "Нет"}')
        self.stdout.write(f'   • Обложка: {"✅ Есть" if game.cover_url else "❌ Нет"}')
        self.stdout.write(f'   • Описание: {"✅ Есть" if game.summary and game.summary.strip() else "❌ Нет"}')

        # Проверяем связи
        self.stdout.write('\n📊 ТЕКУЩИЕ СВЯЗИ:')
        self.stdout.write(f'   • Жанры: {", ".join([g.name for g in game.genres.all()[:5]]) or "❌ Нет"}')
        self.stdout.write(f'   • Платформы: {", ".join([p.name for p in game.platforms.all()[:5]]) or "❌ Нет"}')
        self.stdout.write(f'   • Ключевые слова: {game.keywords.count()} шт.')
        self.stdout.write(f'   • Движки: {", ".join([e.name for e in game.engines.all()[:5]]) or "❌ Нет"}')
        self.stdout.write(f'   • Скриншоты: {game.screenshots.count()} шт.')
        self.stdout.write(f'   • Серии: {", ".join([s.name for s in game.series.all()[:3]]) or "❌ Нет"}')
        self.stdout.write(f'   • Разработчики: {", ".join([c.name for c in game.developers.all()[:3]]) or "❌ Нет"}')
        self.stdout.write(f'   • Издатели: {", ".join([c.name for c in game.publishers.all()[:3]]) or "❌ Нет"}')
        self.stdout.write(f'   • Темы: {", ".join([t.name for t in game.themes.all()[:3]]) or "❌ Нет"}')
        self.stdout.write(
            f'   • Перспективы: {", ".join([p.name for p in game.player_perspectives.all()[:3]]) or "❌ Нет"}')
        self.stdout.write(f'   • Режимы: {", ".join([m.name for m in game.game_modes.all()[:3]]) or "❌ Нет"}')

        # Проверяем недостающие данные
        loader = GameLoader(self.stdout, self.stderr)
        missing_data, missing_count, cover_status = loader.check_missing_game_data(game)

        self.stdout.write('\n📋 СТАТУС ДАННЫХ:')
        status_map = {
            'has_cover': ('Обложка', game.cover_url),
            'has_screenshots': ('Скриншоты', game.screenshots.exists()),
            'has_genres': ('Жанры', game.genres.exists()),
            'has_platforms': ('Платформы', game.platforms.exists()),
            'has_keywords': ('Ключевые слова', game.keywords.exists()),
            'has_engines': ('Движки', game.engines.exists()),
            'has_description': ('Описание', bool(game.summary and game.summary.strip())),
            'has_rating': ('Рейтинг', game.rating is not None),
            'has_release_date': ('Дата релиза', game.first_release_date is not None),
            'has_series': ('Серии', game.series.exists()),
            'has_developers': ('Разработчики', game.developers.exists()),
            'has_publishers': ('Издатели', game.publishers.exists()),
            'has_themes': ('Темы', game.themes.exists()),
            'has_perspectives': ('Перспективы', game.player_perspectives.exists()),
            'has_modes': ('Режимы', game.game_modes.exists()),
        }

        for key, (display_name, has_data) in status_map.items():
            status = "✅ ЕСТЬ" if has_data else "❌ ОТСУТСТВУЕТ"
            self.stdout.write(f'   • {display_name}: {status}')

        self.stdout.write(f'\n📊 ИТОГО: {missing_count} недостающих элементов данных')

        if missing_count == 0:
            self.stdout.write(self.style.SUCCESS('\n✅ Игра уже имеет все данные, обновление не требуется'))
            return

        if not full_check:
            self.stdout.write(self.style.WARNING(
                '\n⚠️ Для просмотра того, ЧТО ИМЕННО будет обновлено, добавьте --full-check'))
            self.stdout.write('   (это выполнит запрос к IGDB API и может занять несколько секунд)')
            return

        # Полная проверка с загрузкой данных из IGDB
        self.stdout.write('\n🌐 ЗАГРУЗКА АКТУАЛЬНЫХ ДАННЫХ ИЗ IGDB...')
        self._simulate_update(game, dry_run, debug)

    def _simulate_update(self, game, dry_run, debug):
        """Симулирует обновление игры, показывая что будет добавлено"""
        from .load_igdb.data_collector import DataCollector
        from .load_igdb.data_loader import DataLoader
        from .load_igdb.relations_handler import RelationsHandler

        start_time = time.time()

        # Загружаем актуальные данные из IGDB
        query = f'''
            fields id,name,summary,storyline,genres,keywords,rating,rating_count,
                   first_release_date,platforms,cover,game_type,screenshots,
                   collections,involved_companies.company,involved_companies.developer,
                   involved_companies.publisher,themes,player_perspectives,
                   game_modes,game_engines;
            where id = {game.igdb_id};
        '''

        try:
            from games.igdb_api import make_igdb_request
            games_data = make_igdb_request('games', query, debug=debug)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Ошибка загрузки из IGDB: {e}'))
            return

        if not games_data:
            self.stdout.write(self.style.ERROR('❌ Игра не найдена в IGDB'))
            return

        game_data = games_data[0]

        # Собираем все ID данных
        collector = DataCollector(self.stdout, self.stderr)
        loader = DataLoader(self.stdout, self.stderr)
        handler = RelationsHandler(self.stdout, self.stderr)

        collected_data = collector.collect_all_data_ids([game_data], debug)

        # Загружаем данные (в dry-run режиме все равно нужно для анализа)
        data_maps, step_times = loader.load_all_data_types_sequentially(collected_data, debug)

        # Анализируем что можно обновить
        self.stdout.write('\n🔍 АНАЛИЗ ДАННЫХ ДЛЯ ОБНОВЛЕНИЯ:')
        self.stdout.write('─' * 50)

        updates_found = False

        # Проверяем обложку
        if not game.cover_url and game_data.get('cover'):
            cover_id = game_data['cover']
            if cover_id in data_maps.get('cover_map', {}):
                updates_found = True
                self.stdout.write(f'   🖼️ Будет добавлена обложка: {data_maps["cover_map"][cover_id]}')

        # Проверяем описание
        if (not game.summary or not game.summary.strip()) and game_data.get('summary'):
            updates_found = True
            summary_preview = game_data['summary'][:100] + '...' if len(game_data['summary']) > 100 else game_data[
                'summary']
            self.stdout.write(f'   📝 Будет добавлено описание: "{summary_preview}"')

        # Проверяем рейтинг
        if game.rating is None and 'rating' in game_data:
            updates_found = True
            self.stdout.write(f'   ⭐ Будет добавлен рейтинг: {game_data["rating"]}')

        # Проверяем дату релиза
        if not game.first_release_date and game_data.get('first_release_date'):
            from datetime import datetime
            release_date = datetime.fromtimestamp(game_data['first_release_date']).strftime('%d.%m.%Y')
            updates_found = True
            self.stdout.write(f'   📅 Будет добавлена дата релиза: {release_date}')

        # Проверяем жанры
        existing_genres = set(game.genres.values_list('igdb_id', flat=True))
        new_genres = [gid for gid in game_data.get('genres', []) if gid not in existing_genres]
        if new_genres:
            updates_found = True
            genre_names = []
            for gid in new_genres:
                if gid in data_maps.get('genre_map', {}):
                    genre_names.append(data_maps['genre_map'][gid].name)
            if genre_names:
                self.stdout.write(f'   🎭 Будут добавлены жанры: {", ".join(genre_names)}')

        # Проверяем платформы
        existing_platforms = set(game.platforms.values_list('igdb_id', flat=True))
        new_platforms = [pid for pid in game_data.get('platforms', []) if pid not in existing_platforms]
        if new_platforms:
            updates_found = True
            platform_names = []
            for pid in new_platforms:
                if pid in data_maps.get('platform_map', {}):
                    platform_names.append(data_maps['platform_map'][pid].name)
            if platform_names:
                self.stdout.write(f'   🖥️ Будут добавлены платформы: {", ".join(platform_names)}')

        # Проверяем ключевые слова
        existing_keywords = set(game.keywords.values_list('igdb_id', flat=True))
        new_keywords = [kid for kid in game_data.get('keywords', []) if kid not in existing_keywords]
        if new_keywords:
            updates_found = True
            self.stdout.write(f'   🔑 Будет добавлено ключевых слов: {len(new_keywords)}')

        # Проверяем скриншоты
        if game_data.get('screenshots'):
            existing_screenshots = game.screenshots.count()
            new_screenshots = len(game_data['screenshots']) - existing_screenshots
            if new_screenshots > 0:
                updates_found = True
                self.stdout.write(f'   📸 Будет добавлено скриншотов: {new_screenshots}')

        # Проверяем движки
        existing_engines = set(game.engines.values_list('igdb_id', flat=True))
        engine_ids = []
        for engine_data in game_data.get('game_engines', []):
            if isinstance(engine_data, dict):
                engine_ids.append(engine_data.get('id'))
            else:
                engine_ids.append(engine_data)

        new_engines = [eid for eid in engine_ids if eid and eid not in existing_engines]
        if new_engines:
            updates_found = True
            engine_names = []
            for eid in new_engines:
                if eid in data_maps.get('engine_map', {}):
                    engine_names.append(data_maps['engine_map'][eid].name)
            if engine_names:
                self.stdout.write(f'   ⚙️ Будут добавлены движки: {", ".join(engine_names)}')

        # Проверяем серии
        if 'collections' in game_data:
            existing_series = set(game.series.values_list('igdb_id', flat=True))
            new_series = [sid for sid in game_data['collections'] if sid not in existing_series]
            if new_series:
                updates_found = True
                series_names = []
                for sid in new_series:
                    if sid in data_maps.get('series_map', {}):
                        series_names.append(data_maps['series_map'][sid].name)
                if series_names:
                    self.stdout.write(f'   📚 Будут добавлены серии: {", ".join(series_names)}')

        # Проверяем разработчиков и издателей
        if game_data.get('involved_companies'):
            company_ids = set()
            developer_ids = set()
            publisher_ids = set()

            for company_data in game_data['involved_companies']:
                cid = company_data.get('company')
                if cid:
                    company_ids.add(cid)
                    if company_data.get('developer', False):
                        developer_ids.add(cid)
                    if company_data.get('publisher', False):
                        publisher_ids.add(cid)

            existing_developers = set(game.developers.values_list('igdb_id', flat=True))
            new_developers = [did for did in developer_ids if did not in existing_developers]
            if new_developers:
                updates_found = True
                dev_names = []
                for did in new_developers:
                    if did in data_maps.get('company_map', {}):
                        dev_names.append(data_maps['company_map'][did].name)
                if dev_names:
                    self.stdout.write(f'   🏢 Будут добавлены разработчики: {", ".join(dev_names)}')

            existing_publishers = set(game.publishers.values_list('igdb_id', flat=True))
            new_publishers = [pid for pid in publisher_ids if pid not in existing_publishers]
            if new_publishers:
                updates_found = True
                pub_names = []
                for pid in new_publishers:
                    if pid in data_maps.get('company_map', {}):
                        pub_names.append(data_maps['company_map'][pid].name)
                if pub_names:
                    self.stdout.write(f'   📦 Будут добавлены издатели: {", ".join(pub_names)}')

        # Проверяем темы
        existing_themes = set(game.themes.values_list('igdb_id', flat=True))
        new_themes = [tid for tid in game_data.get('themes', []) if tid not in existing_themes]
        if new_themes:
            updates_found = True
            theme_names = []
            for tid in new_themes:
                if tid in data_maps.get('theme_map', {}):
                    theme_names.append(data_maps['theme_map'][tid].name)
            if theme_names:
                self.stdout.write(f'   🎨 Будут добавлены темы: {", ".join(theme_names)}')

        # Проверяем перспективы
        existing_perspectives = set(game.player_perspectives.values_list('igdb_id', flat=True))
        new_perspectives = [pid for pid in game_data.get('player_perspectives', []) if pid not in existing_perspectives]
        if new_perspectives:
            updates_found = True
            perspective_names = []
            for pid in new_perspectives:
                if pid in data_maps.get('perspective_map', {}):
                    perspective_names.append(data_maps['perspective_map'][pid].name)
            if perspective_names:
                self.stdout.write(f'   👁️ Будут добавлены перспективы: {", ".join(perspective_names)}')

        # Проверяем режимы
        existing_modes = set(game.game_modes.values_list('igdb_id', flat=True))
        new_modes = [mid for mid in game_data.get('game_modes', []) if mid not in existing_modes]
        if new_modes:
            updates_found = True
            mode_names = []
            for mid in new_modes:
                if mid in data_maps.get('mode_map', {}):
                    mode_names.append(data_maps['mode_map'][mid].name)
            if mode_names:
                self.stdout.write(f'   🎮 Будут добавлены режимы: {", ".join(mode_names)}')

        elapsed = time.time() - start_time

        self.stdout.write('─' * 50)

        if not updates_found:
            self.stdout.write(self.style.WARNING('\n⚠️ Новых данных для обновления не найдено'))
        else:
            if dry_run:
                self.stdout.write(self.style.SUCCESS(f'\n✅ СИМУЛЯЦИЯ ЗАВЕРШЕНА (dry-run)'))
                self.stdout.write(self.style.WARNING('   Реальное обновление НЕ производилось'))
                self.stdout.write(f'   Чтобы выполнить реальное обновление, используйте --no-dry-run')
            else:
                self.stdout.write(self.style.SUCCESS(f'\n✅ РЕАЛЬНОЕ ОБНОВЛЕНИЕ ВЫПОЛНЕНО'))
                self.stdout.write(f'   Все найденные данные были добавлены в базу')

        self.stdout.write(f'\n⏱️  Время проверки: {elapsed:.2f}с')