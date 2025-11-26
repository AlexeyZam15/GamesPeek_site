# update_games_with_additional_data.py
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from games.models import (
    Game, Series, Company, Theme, PlayerPerspective, GameMode
)
from games.igdb_api import make_igdb_request
import time
from collections import defaultdict


class Command(BaseCommand):
    help = 'Обновляет игры с дополнительными данными: серии, разработчики, издатели, темы, перспективы, режимы'

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch-size',
            type=int,
            default=50,
            help='Количество игр для обработки за один запрос к IGDB'
        )
        parser.add_argument(
            '--delay',
            type=float,
            default=0.2,
            help='Задержка между запросами к IGDB (в секундах)'
        )
        parser.add_argument(
            '--start-from',
            type=int,
            default=0,
            help='Начать с определенного индекса игры'
        )
        parser.add_argument(
            '--game-ids',
            type=str,
            help='Обновить только конкретные игры (через запятую)'
        )
        parser.add_argument(
            '--limit',
            type=int,
            help='Ограничить количество обрабатываемых игр'
        )
        parser.add_argument(
            '--skip-existing',
            action='store_true',
            help='Пропускать игры которые уже имеют данные'
        )
        parser.add_argument(
            '--debug',
            action='store_true',
            help='Показать отладочную информацию о данных от IGDB'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Показать какие данные будут обновлены без сохранения в базу'
        )
        parser.add_argument(
            '--skip-no-data',
            action='store_true',
            help='Пропускать игры для которых в IGDB нет данных'
        )

    def handle(self, *args, **options):
        if not any([options['game_ids'], options['limit'], options['start_from'] > 0, options['skip_existing']]):
            self.stdout.write(
                "ℹ️ Запуск без параметров пропущен. Используйте --game-ids, --limit, --start-from или --skip-existing")
            return
        self.batch_size = options['batch_size']
        self.delay = options['delay']
        self.dry_run = options['dry_run']
        self.debug = options['debug']
        self.skip_existing = options['skip_existing']
        self.skip_no_data = options['skip_no_data']
        start_from = options['start_from']
        game_ids = options['game_ids']
        limit = options['limit']

        if self.dry_run:
            self.stdout.write(
                self.style.WARNING(
                    "🚧 РЕЖИМ DRY RUN - данные не будут сохранены в базу!"
                )
            )

        if self.debug:
            self.stdout.write(
                self.style.WARNING(
                    "🐛 РЕЖИМ DEBUG - будут показаны сырые данные от IGDB"
                )
            )

        # Получаем игры для обновления
        if game_ids:
            game_ids_list = [int(id.strip()) for id in game_ids.split(',')]
            games = Game.objects.filter(igdb_id__in=game_ids_list)
            self.stdout.write(f"Будет обработано {len(games)} конкретных игр...")
        else:
            games = Game.objects.all().order_by('id')
            if self.skip_existing:
                # Пропускаем игры у которых уже есть данные
                games = games.filter(
                    series__isnull=True,
                    developers__isnull=True,
                    publishers__isnull=True
                )
                self.stdout.write("⏩ Пропускаем игры с уже заполненными данными")

            if limit:
                games = games[start_from:start_from + limit]
                self.stdout.write(f"Будет обработано {limit} игр из базы (начиная с {start_from})...")
            else:
                games = games[start_from:]
                total_count = games.count()
                self.stdout.write(f"Будет обработано {total_count} игр из базы (начиная с {start_from})...")

        total_games = games.count()
        if total_games == 0:
            self.stdout.write("🤷 Нет игр для обработки")
            return

        successful_updates = 0
        failed_updates = 0
        skipped_no_data = 0

        # Предзагружаем существующие данные для избежания дубликатов
        self.preload_existing_data()

        # Обрабатываем игры батчами
        for i in range(0, total_games, self.batch_size):
            batch = list(games[i:i + self.batch_size])
            batch_ids = [game.igdb_id for game in batch]

            self.stdout.write(
                f"\n📦 Батч {i // self.batch_size + 1}: игры {i + 1}-{min(i + self.batch_size, total_games)} из {total_games}"
            )

            try:
                updated_count, skipped_count = self.process_batch(batch_ids, batch)
                successful_updates += updated_count
                skipped_no_data += skipped_count

                if not self.dry_run:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"✅ Обновлено {updated_count} игр"
                        )
                    )
                    if skipped_count > 0:
                        self.stdout.write(
                            self.style.WARNING(
                                f"⏭️  Пропущено {skipped_count} игр (нет данных в IGDB)"
                            )
                        )

            except Exception as e:
                failed_updates += len(batch)
                self.stdout.write(
                    self.style.ERROR(
                        f"❌ Ошибка батча: {e}"
                    )
                )

            # Короткая задержка между запросами
            if i + self.batch_size < total_games and not self.dry_run:
                time.sleep(self.delay)

        self.stdout.write(
            self.style.SUCCESS(
                f"\n🎉 Завершено! Успешно: {successful_updates}, Ошибок: {failed_updates}"
            )
        )
        if skipped_no_data > 0:
            self.stdout.write(
                self.style.WARNING(
                    f"⏭️  Пропущено (нет данных): {skipped_no_data}"
                )
            )

        if self.dry_run:
            self.stdout.write(
                self.style.WARNING(
                    "💡 Для реального обновления запустите команду без --dry-run"
                )
            )

    def preload_existing_data(self):
        """Предзагружаем существующие данные для оптимизации"""
        self.stdout.write("⚡ Предзагрузка данных...")

        # Кэшируем существующие серии, компании и т.д.
        self.existing_series = {s.igdb_id: s for s in Series.objects.all()}
        self.existing_companies = {c.igdb_id: c for c in Company.objects.all()}
        self.existing_themes = {t.igdb_id: t for t in Theme.objects.all()}
        self.existing_perspectives = {p.igdb_id: p for p in PlayerPerspective.objects.all()}
        self.existing_modes = {m.igdb_id: m for m in GameMode.objects.all()}

        self.stdout.write(f"📚 Серии: {len(self.existing_series)}")
        self.stdout.write(f"🏢 Компании: {len(self.existing_companies)}")
        self.stdout.write(f"🎨 Темы: {len(self.existing_themes)}")
        self.stdout.write(f"👁️ Перспективы: {len(self.existing_perspectives)}")
        self.stdout.write(f"🎮 Режимы: {len(self.existing_modes)}")

    def process_batch(self, batch_ids, batch_games):
        """Обрабатывает батч игр"""
        # Получаем все данные за один запрос
        games_data = self.fetch_games_data_from_igdb(batch_ids)

        if not games_data:
            return 0, 0

        if self.debug:
            self.stdout.write("\n🔍 ОТЛАДОЧНЫЕ ДАННЫЕ ОТ IGDB:")
            for game_data in games_data:
                self.stdout.write(f"Игра: {game_data.get('name')} (ID: {game_data.get('id')})")
                self.stdout.write(f"  collections: {game_data.get('collections')}")
                self.stdout.write(f"  involved_companies: {len(game_data.get('involved_companies', []))}")
                self.stdout.write(f"  themes: {game_data.get('themes')}")
                self.stdout.write("-" * 30)

        # Собираем все ID для массовых запросов
        all_collection_ids = set()
        all_company_ids = set()
        all_theme_ids = set()
        all_perspective_ids = set()
        all_mode_ids = set()

        for game_data in games_data:
            # Коллекции
            if game_data.get('collections'):
                all_collection_ids.update(game_data['collections'])

            # Компании (разработчики и издатели)
            if game_data.get('involved_companies'):
                for company in game_data['involved_companies']:
                    all_company_ids.add(company['company'])

            # Темы, перспективы, режимы
            if game_data.get('themes'):
                all_theme_ids.update(game_data['themes'])
            if game_data.get('player_perspectives'):
                all_perspective_ids.update(game_data['player_perspectives'])
            if game_data.get('game_modes'):
                all_mode_ids.update(game_data['game_modes'])

        # Массово получаем все данные
        collections_data = self.fetch_collections_data(list(all_collection_ids))
        companies_data = self.fetch_companies_data(list(all_company_ids))
        themes_data = self.fetch_themes_data(list(all_theme_ids))
        perspectives_data = self.fetch_perspectives_data(list(all_perspective_ids))
        modes_data = self.fetch_game_modes_data(list(all_mode_ids))

        # Создаем объекты в базе (если нужно)
        created_series = self.bulk_create_series(collections_data)
        created_companies = self.bulk_create_companies(companies_data)
        created_themes = self.bulk_create_themes(themes_data)
        created_perspectives = self.bulk_create_perspectives(perspectives_data)
        created_modes = self.bulk_create_modes(modes_data)

        # Обновляем кэш
        self.existing_series.update(created_series)
        self.existing_companies.update(created_companies)
        self.existing_themes.update(created_themes)
        self.existing_perspectives.update(created_perspectives)
        self.existing_modes.update(created_modes)

        # Обновляем игры
        updated_count = 0
        skipped_count = 0

        for game_data in games_data:
            try:
                # Проверяем, есть ли вообще данные для обновления
                has_data = self.has_additional_data(game_data)

                if not has_data and self.skip_no_data:
                    if self.debug:
                        self.stdout.write(f"⏭️  Пропуск {game_data.get('name')} - нет данных в IGDB")
                    skipped_count += 1
                    continue

                if self.dry_run:
                    self.dry_run_update_game(game_data, batch_games)
                else:
                    self.update_game_with_additional_data(
                        game_data, batch_games,
                        collections_data, companies_data,
                        themes_data, perspectives_data, modes_data
                    )
                updated_count += 1
            except Exception as e:
                self.stdout.write(f"⚠️ Ошибка игры {game_data.get('id')}: {e}")

        return updated_count, skipped_count

    def has_additional_data(self, game_data):
        """Проверяет, есть ли дополнительные данные в IGDB"""
        has_collections = bool(game_data.get('collections'))
        has_companies = bool(game_data.get('involved_companies'))
        has_themes = bool(game_data.get('themes'))
        has_perspectives = bool(game_data.get('player_perspectives'))
        has_modes = bool(game_data.get('game_modes'))

        return any([has_collections, has_companies, has_themes, has_perspectives, has_modes])

    def update_game_with_additional_data(self, game_data, batch_games,
                                         collections_data, companies_data,
                                         themes_data, perspectives_data, modes_data):
        """Обновляет игру с дополнительными данными"""
        igdb_id = game_data['id']

        # Находим игру в базе
        try:
            game = next(g for g in batch_games if g.igdb_id == igdb_id)
        except StopIteration:
            return

        with transaction.atomic():
            updated = False

            # Серия
            if game_data.get('collections'):
                collection_id = game_data['collections'][0]
                series = self.existing_series.get(collection_id)
                if series and game.series != series:
                    game.series = series
                    updated = True

            # Разработчики и издатели
            developer_ids = []
            publisher_ids = []

            if game_data.get('involved_companies'):
                for company_data in game_data['involved_companies']:
                    company_id = company_data['company']
                    if company_data.get('developer', False):
                        developer_ids.append(company_id)
                    if company_data.get('publisher', False):
                        publisher_ids.append(company_id)

                # Получаем объекты компаний из кэша
                developers = [self.existing_companies[cid] for cid in developer_ids if cid in self.existing_companies]
                publishers = [self.existing_companies[cid] for cid in publisher_ids if cid in self.existing_companies]

                if developers:
                    current_devs = set(game.developers.all())
                    new_devs = set(developers)
                    if current_devs != new_devs:
                        game.developers.set(developers)
                        updated = True

                if publishers:
                    current_pubs = set(game.publishers.all())
                    new_pubs = set(publishers)
                    if current_pubs != new_pubs:
                        game.publishers.set(publishers)
                        updated = True

            # Темы, перспективы, режимы
            if game_data.get('themes'):
                themes = [self.existing_themes[tid] for tid in game_data['themes'] if tid in self.existing_themes]
                if themes:
                    current_themes = set(game.themes.all())
                    new_themes = set(themes)
                    if current_themes != new_themes:
                        game.themes.set(themes)
                        updated = True

            if game_data.get('player_perspectives'):
                perspectives = [self.existing_perspectives[pid] for pid in game_data['player_perspectives'] if
                                pid in self.existing_perspectives]
                if perspectives:
                    current_perspectives = set(game.player_perspectives.all())
                    new_perspectives = set(perspectives)
                    if current_perspectives != new_perspectives:
                        game.player_perspectives.set(perspectives)
                        updated = True

            if game_data.get('game_modes'):
                modes = [self.existing_modes[mid] for mid in game_data['game_modes'] if mid in self.existing_modes]
                if modes:
                    current_modes = set(game.game_modes.all())
                    new_modes = set(modes)
                    if current_modes != new_modes:
                        game.game_modes.set(modes)
                        updated = True

            if updated:
                game.save()
                if self.debug:
                    self.stdout.write(f"🔄 Обновлена игра: {game.name}")

    def dry_run_update_game(self, game_data, batch_games):
        """Показывает какие данные будут обновлены без сохранения"""
        igdb_id = game_data['id']

        # Находим игру в базе
        try:
            game = next(g for g in batch_games if g.igdb_id == igdb_id)
        except StopIteration:
            self.stdout.write(f"⚠️ Игра {igdb_id} не найдена в текущем батче")
            return

        self.stdout.write(f"\n🎮 ИГРА: {game.name} (ID: {igdb_id})")
        self.stdout.write("-" * 40)

        # Серия
        if game_data.get('collections'):
            collection_id = game_data['collections'][0]
            series_data = self.fetch_collections_data([collection_id])
            if series_data:
                series_name = series_data[0].get('name', 'Unknown')
                self.stdout.write(f"📚 Серия: БУДЕТ ДОБАВЛЕНА - {series_name} (ID: {collection_id})")
            else:
                self.stdout.write(f"📚 Серия: не удалось получить данные для ID {collection_id}")
        else:
            self.stdout.write("📚 Серия: нет данных в IGDB")

        # Разработчики
        developer_names = []
        if game_data.get('involved_companies'):
            developer_ids = []
            for company in game_data['involved_companies']:
                if company.get('developer', False):
                    developer_ids.append(company['company'])

            if developer_ids:
                developers_data = self.fetch_companies_data(developer_ids)
                developer_names = [d.get('name', 'Unknown') for d in developers_data]
                self.stdout.write(f"🏢 Разработчики: БУДУТ ДОБАВЛЕНЫ - {', '.join(developer_names)}")
            else:
                self.stdout.write("🏢 Разработчики: нет developer компаний")
        else:
            self.stdout.write("🏢 Разработчики: нет данных в IGDB")

        # Издатели
        publisher_names = []
        if game_data.get('involved_companies'):
            publisher_ids = []
            for company in game_data['involved_companies']:
                if company.get('publisher', False):
                    publisher_ids.append(company['company'])

            if publisher_ids:
                publishers_data = self.fetch_companies_data(publisher_ids)
                publisher_names = [p.get('name', 'Unknown') for p in publishers_data]
                self.stdout.write(f"📦 Издатели: БУДУТ ДОБАВЛЕНЫ - {', '.join(publisher_names)}")
            else:
                self.stdout.write("📦 Издатели: нет publisher компаний")
        else:
            self.stdout.write("📦 Издатели: нет данных в IGDB")

        # Темы
        theme_names = []
        if game_data.get('themes'):
            themes_data = self.fetch_themes_data(game_data['themes'])
            theme_names = [t.get('name', 'Unknown') for t in themes_data]
            self.stdout.write(f"🎨 Темы: БУДУТ ДОБАВЛЕНЫ - {', '.join(theme_names)}")
        else:
            self.stdout.write("🎨 Темы: нет данных в IGDB")

        # Перспективы
        perspective_names = []
        if game_data.get('player_perspectives'):
            perspectives_data = self.fetch_perspectives_data(game_data['player_perspectives'])
            perspective_names = [p.get('name', 'Unknown') for p in perspectives_data]
            self.stdout.write(f"👁️ Перспективы: БУДУТ ДОБАВЛЕНЫ - {', '.join(perspective_names)}")
        else:
            self.stdout.write("👁️ Перспективы: нет данных в IGDB")

        # Режимы игры
        mode_names = []
        if game_data.get('game_modes'):
            modes_data = self.fetch_game_modes_data(game_data['game_modes'])
            mode_names = [m.get('name', 'Unknown') for m in modes_data]
            self.stdout.write(f"🎮 Режимы: БУДУТ ДОБАВЛЕНЫ - {', '.join(mode_names)}")
        else:
            self.stdout.write("🎮 Режимы: нет данных в IGDB")

        # Текущие данные в базе (для сравнения)
        self.stdout.write("\n📊 ТЕКУЩИЕ ДАННЫЕ В БАЗЕ:")
        self.stdout.write(f"📚 Серия: {game.series.name if game.series else 'нет'}")
        self.stdout.write(
            f"🏢 Разработчики: {', '.join(game.developer_names) if hasattr(game, 'developers') and game.developers.exists() else 'нет'}")
        self.stdout.write(
            f"📦 Издатели: {', '.join(game.publisher_names) if hasattr(game, 'publishers') and game.publishers.exists() else 'нет'}")
        self.stdout.write(
            f"🎨 Темы: {', '.join(game.theme_names) if hasattr(game, 'themes') and game.themes.exists() else 'нет'}")
        self.stdout.write(
            f"👁️ Перспективы: {', '.join(game.perspective_names) if hasattr(game, 'player_perspectives') and game.player_perspectives.exists() else 'нет'}")
        self.stdout.write(
            f"🎮 Режимы: {', '.join(game.game_mode_names) if hasattr(game, 'game_modes') and game.game_modes.exists() else 'нет'}")

    # Массовые методы создания объектов
    def bulk_create_series(self, collections_data):
        """Массово создает серии"""
        new_series = []
        for data in collections_data:
            if data['id'] not in self.existing_series:
                series = Series(
                    igdb_id=data['id'],
                    name=data.get('name', ''),
                    description=''
                )
                new_series.append(series)

        if new_series and not self.dry_run:
            Series.objects.bulk_create(new_series)
            self.stdout.write(f"📚 Создано {len(new_series)} новых серий")

        return {s.igdb_id: s for s in new_series}

    def bulk_create_companies(self, companies_data):
        """Массово создает компании"""
        new_companies = []
        for data in companies_data:
            if data['id'] not in self.existing_companies:
                company = Company(
                    igdb_id=data['id'],
                    name=data.get('name', ''),
                    description=data.get('description', ''),
                    country=data.get('country'),
                    logo_url=f"https://images.igdb.com/igdb/image/upload/t_logo_med/{data['logo']['image_id']}.png" if data.get(
                        'logo') else '',
                    website=data.get('url', ''),
                )
                new_companies.append(company)

        if new_companies and not self.dry_run:
            Company.objects.bulk_create(new_companies)
            self.stdout.write(f"🏢 Создано {len(new_companies)} новых компаний")

        return {c.igdb_id: c for c in new_companies}

    def bulk_create_themes(self, themes_data):
        """Массово создает темы"""
        return self._bulk_create_simple_objects(Theme, themes_data, "🎨 Тем")

    def bulk_create_perspectives(self, perspectives_data):
        """Массово создает перспективы"""
        return self._bulk_create_simple_objects(PlayerPerspective, perspectives_data, "👁️ Перспектив")

    def bulk_create_modes(self, modes_data):
        """Массово создает режимы"""
        return self._bulk_create_simple_objects(GameMode, modes_data, "🎮 Режимов")

    def _bulk_create_simple_objects(self, model, data_list, object_type):
        """Универсальный метод для массового создания простых объектов"""
        # Исправляем имя кэша для разных моделей
        if model == Theme:
            existing_cache = self.existing_themes
        elif model == PlayerPerspective:
            existing_cache = self.existing_perspectives
        elif model == GameMode:
            existing_cache = self.existing_modes
        else:
            return {}

        new_objects = []
        for data in data_list:
            if data['id'] not in existing_cache:
                obj = model(
                    igdb_id=data['id'],
                    name=data.get('name', '')
                )
                new_objects.append(obj)

        if new_objects and not self.dry_run:
            model.objects.bulk_create(new_objects)
            self.stdout.write(f"{object_type}: создано {len(new_objects)}")

        return {obj.igdb_id: obj for obj in new_objects}

    # Методы для получения данных из IGDB
    def fetch_games_data_from_igdb(self, game_ids):
        """Получает полные данные об играх из IGDB"""
        if not game_ids:
            return []

        fields = """
            id,name,collections,franchises,involved_companies.company,
            involved_companies.developer,involved_companies.publisher,
            themes,player_perspectives,game_modes
        """

        # Исправляем формирование запроса для случая с одним ID
        if len(game_ids) == 1:
            query = f"""
                fields {fields};
                where id = {game_ids[0]};
                limit 1;
            """
        else:
            query = f"""
                fields {fields};
                where id = ({",".join(map(str, game_ids))});
                limit {len(game_ids)};
            """

        try:
            return make_igdb_request('games', query)
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ Ошибка запроса к IGDB: {e}")
            )
            return []

    def fetch_collections_data(self, collection_ids):
        """Получает данные о коллекциях из IGDB"""
        if not collection_ids:
            return []

        fields = "id,name"
        query = f'fields {fields}; where id = ({",".join(map(str, collection_ids))});'
        try:
            return make_igdb_request('collections', query)
        except Exception as e:
            self.stdout.write(f"⚠️ Ошибка при получении данных коллекций: {e}")
            return []

    def fetch_companies_data(self, company_ids):
        """Получает данные о компаниях из IGDB"""
        if not company_ids:
            return []

        fields = "id,name,description,country,logo.image_id,url"
        query = f'fields {fields}; where id = ({",".join(map(str, company_ids))});'
        try:
            return make_igdb_request('companies', query)
        except Exception as e:
            self.stdout.write(f"⚠️ Ошибка при получении данных компаний: {e}")
            return []

    def fetch_themes_data(self, theme_ids):
        """Получает данные о темах из IGDB"""
        return self._fetch_simple_objects('themes', theme_ids)

    def fetch_perspectives_data(self, perspective_ids):
        """Получает данные о перспективах из IGDB"""
        return self._fetch_simple_objects('player_perspectives', perspective_ids)

    def fetch_game_modes_data(self, mode_ids):
        """Получает данные о режимах игры из IGDB"""
        return self._fetch_simple_objects('game_modes', mode_ids)

    def _fetch_simple_objects(self, endpoint, object_ids):
        """Универсальный метод для получения простых объектов"""
        if not object_ids:
            return []

        fields = "id,name"
        query = f'fields {fields}; where id = ({",".join(map(str, object_ids))});'
        try:
            return make_igdb_request(endpoint, query)
        except:
            return []
