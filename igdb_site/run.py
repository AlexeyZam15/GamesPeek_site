#!/usr/bin/env python
"""
Desktop launcher for Django + PostgreSQL.
Users run this .exe and open browser to use the site.
"""
import os
import sys
import atexit
from pathlib import Path


# ============================================
# PATCH: DISABLE TIMEZONE FOR PGEMBED
# ============================================

def patch_postgresql_timezone():
    """Монки-патч для отключения установки часового пояса в PostgreSQL."""
    try:
        from django.db.backends.postgresql import base

        def patched_init_connection_state(self):
            pass

        base.DatabaseWrapper.init_connection_state = patched_init_connection_state
        print("[PATCH] PostgreSQL timezone patch applied")
    except Exception as e:
        print(f"[PATCH] Failed to apply timezone patch: {e}")


patch_postgresql_timezone()


def setup_environment():
    """Configure Django environment before any imports."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'igdb_site.settings')
    os.environ['DESKTOP_MODE'] = '1'

    if getattr(sys, 'frozen', False):
        exe_dir = Path(sys.executable).parent
        sys.path.insert(0, str(exe_dir))
    else:
        current_dir = Path(__file__).parent
        sys.path.insert(0, str(current_dir))
        sys.path.insert(0, str(current_dir / 'igdb_site'))


def start_postgresql():
    """
    Start embedded PostgreSQL server.
    Data persists in user's AppData folder between launches.
    """
    try:
        import pgembed
    except ImportError:
        print("ERROR: pgembed not installed. Run: pip install pgembed")
        sys.exit(1)

    if sys.platform == 'win32':
        appdata = os.environ.get('APPDATA', os.path.expanduser('~'))
        data_dir = os.path.join(appdata, 'IGDB_Site_PostgreSQL')
    else:
        data_dir = os.path.join(os.path.expanduser('~'), '.igdb_site_postgresql')

    # Не удаляем базу данных при запуске
    # База данных удаляется только при сборке в build_exe.py

    Path(data_dir).mkdir(parents=True, exist_ok=True)

    print(f"📁 Database stored at: {data_dir}")

    server = pgembed.get_server(data_dir)
    database_url = server.get_uri()

    # Отключаем SSL для pgembed
    if database_url.startswith('postgresql://'):
        database_url = database_url.replace('postgresql://', 'postgres://')

    # Добавляем параметр отключения SSL
    if '?' in database_url:
        database_url += '&sslmode=disable'
    else:
        database_url += '?sslmode=disable'

    atexit.register(lambda: server.cleanup())

    print("✅ PostgreSQL started")
    return database_url


def run_migrations_once():
    """
    Run desktop-specific migration to create ALL tables, then import data.
    Optimized for maximum speed using parallel processing and optimized queries.
    """
    from django.db import connection, transaction
    from django.core.management import call_command
    from django.contrib.auth import get_user_model
    import json
    from tqdm import tqdm
    import gc
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    # Сначала применяем стандартные миграции Django
    print("📦 Applying Django core migrations...")
    call_command('migrate', interactive=False)
    print("✅ Core migrations completed")

    # Проверяем наличие таблицы games_game и данных в ней
    with connection.cursor() as cursor:
        cursor.execute("""
                       SELECT EXISTS (SELECT 1
                                      FROM information_schema.tables
                                      WHERE table_name = 'games_game');
                       """)
        has_games_table = cursor.fetchone()[0]

        has_games_data = False
        if has_games_table:
            cursor.execute("SELECT COUNT(*) FROM games_game;")
            game_count = cursor.fetchone()[0]
            has_games_data = game_count > 0
            print(f"📊 Current games in database: {game_count}")

    if not has_games_table:
        print("📦 First run: creating database tables using desktop migration...")
        call_command('migrate', 'desktop_migrations', interactive=False)
        print("✅ Desktop migrations completed")
        needs_import = True
    elif not has_games_data:
        print("📦 Tables exist but no data found. Starting data import...")
        needs_import = True
    else:
        print("✅ Database tables exist with data, skipping migrations and import")
        needs_import = False

    if needs_import:
        # Определяем путь к data.json
        if getattr(sys, 'frozen', False):
            exe_dir = Path(sys.executable).parent
        else:
            exe_dir = Path.cwd()

        data_file = exe_dir / 'data.json'

        if data_file.exists():
            file_size_mb = data_file.stat().st_size / 1024 / 1024
            print(f"📦 Found data.json ({file_size_mb:.1f} MB)")
            print("📖 Loading JSON file...")

            try:
                # Загружаем JSON с прогресс-баром
                with tqdm(total=100, desc="  ⏳ Loading JSON", unit="%",
                          bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}') as pbar:
                    with open(data_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    pbar.update(100)

                print(f"✅ JSON loaded successfully")
                print(f"📊 Found {len(data)} model types in fixture")

                with connection.cursor() as cursor:
                    cursor.execute('SET CONSTRAINTS ALL DEFERRED')
                    cursor.execute('SET synchronous_commit TO OFF')
                    cursor.execute('SET maintenance_work_mem TO "1GB"')
                    cursor.execute('SET work_mem TO "256MB"')

                total_objects = 0
                import_start_time = time.time()

                # Импортируем модели из games.models
                from games import models as game_models

                # ИМПОРТ KEYWORD CATEGORY
                if 'KeywordCategory' in data and data['KeywordCategory']:
                    objects_data = data['KeywordCategory']
                    print(f"\n  ⚡ Importing KeywordCategory: {len(objects_data)} records...")

                    objects_to_create = []
                    for item in objects_data:
                        fields = item['fields'].copy()
                        objects_to_create.append(game_models.KeywordCategory(id=item['pk'], **fields))

                    batch_size = 5000
                    saved_count = 0
                    with tqdm(total=len(objects_to_create), desc="  ⏳ Saving", unit="rec",
                              bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
                        for i in range(0, len(objects_to_create), batch_size):
                            batch = objects_to_create[i:i + batch_size]
                            created = game_models.KeywordCategory.objects.bulk_create(batch, ignore_conflicts=True)
                            saved_count += len(created)
                            total_objects += len(created)
                            pbar.update(len(batch))
                    print(f"    ✅ Saved: {saved_count}/{len(objects_data)} records")
                    del objects_to_create
                    gc.collect()

                # ИМПОРТ НЕЗАВИСИМЫХ МОДЕЛЕЙ (без внешних ключей)
                independent_models = {
                    'PlayerPerspective': game_models.PlayerPerspective,
                    'GameMode': game_models.GameMode,
                    'Theme': game_models.Theme,
                    'Genre': game_models.Genre,
                    'Platform': game_models.Platform,
                    'GameEngine': game_models.GameEngine,
                    'Company': game_models.Company,
                    'Series': game_models.Series,
                }

                for data_key, model_class in independent_models.items():
                    if data_key in data and data[data_key]:
                        objects_data = data[data_key]
                        print(f"\n  ⚡ Importing {data_key}: {len(objects_data)} records...")

                        objects_to_create = []
                        for item in objects_data:
                            fields = item['fields'].copy()
                            objects_to_create.append(model_class(id=item['pk'], **fields))

                        batch_size = 10000
                        saved_count = 0
                        with tqdm(total=len(objects_to_create), desc="  ⏳ Saving", unit="rec",
                                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
                            for i in range(0, len(objects_to_create), batch_size):
                                batch = objects_to_create[i:i + batch_size]
                                created = model_class.objects.bulk_create(batch, ignore_conflicts=True)
                                saved_count += len(created)
                                total_objects += len(created)
                                pbar.update(len(batch))
                        print(f"    ✅ Saved: {saved_count}/{len(objects_data)} records")
                        del objects_to_create
                        gc.collect()

                # ИМПОРТ KEYWORD (имеет ForeignKey на KeywordCategory)
                if 'Keyword' in data and data['Keyword']:
                    objects_data = data['Keyword']
                    print(f"\n  ⚡ Importing Keyword: {len(objects_data)} records...")

                    # Используем raw SQL для максимальной скорости
                    values_list = []
                    for item in objects_data:
                        fields = item['fields']
                        pk = item['pk']
                        igdb_id = fields.get('igdb_id', 0)
                        name = fields.get('name', '').replace("'", "''")
                        cached_usage_count = fields.get('cached_usage_count', 0)
                        category_id = fields.get('category')
                        created_at = fields.get('created_at', 'NOW()')

                        category_sql = str(category_id) if category_id else 'NULL'
                        values_list.append(
                            f"({pk}, {igdb_id}, '{name}', {cached_usage_count}, {category_sql}, '{created_at}')")

                    if values_list:
                        batch_size = 10000
                        inserted = 0
                        with tqdm(total=len(values_list), desc="  ⏳ Saving", unit="rec",
                                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
                            with connection.cursor() as cursor:
                                for i in range(0, len(values_list), batch_size):
                                    batch = values_list[i:i + batch_size]
                                    sql = f"""
                                        INSERT INTO games_keyword (id, igdb_id, name, cached_usage_count, category_id, created_at)
                                        VALUES {','.join(batch)}
                                        ON CONFLICT (id) DO NOTHING
                                    """
                                    cursor.execute(sql)
                                    inserted += cursor.rowcount
                                    pbar.update(len(batch))
                        print(f"    ✅ Saved: {inserted}/{len(objects_data)} records")
                        total_objects += inserted
                    del values_list
                    gc.collect()

                # ИМПОРТ GAME - МАКСИМАЛЬНО БЫСТРЫЙ
                if 'Game' in data and data['Game']:
                    games_data = data['Game']
                    print(f"\n  🚀 Importing Game: {len(games_data)} records...")

                    # Сортируем игры: сначала без parent_game и version_parent
                    games_data_sorted = sorted(
                        games_data,
                        key=lambda x: (
                            (x['fields'].get('parent_game') is not None) or (
                                    x['fields'].get('version_parent') is not None),
                            x['fields'].get('parent_game') or 0,
                            x['fields'].get('version_parent') or 0
                        )
                    )

                    # Подготавливаем данные для raw SQL INSERT игр
                    game_values = []
                    game_m2m_data = []
                    game_parent_refs = []

                    # Функция для безопасного преобразования значений в SQL
                    def sql_value(val, is_string=False):
                        if val is None:
                            return 'NULL'
                        if is_string:
                            if val == '':
                                return "''"
                            escaped = str(val).replace("'", "''")
                            return f"'{escaped}'"
                        if isinstance(val, bool):
                            return 'TRUE' if val else 'FALSE'
                        return str(val)

                    with tqdm(total=len(games_data_sorted), desc="  ⏳ Preparing", unit="rec",
                              bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}') as pbar:
                        for game_data in games_data_sorted:
                            game_id = game_data['pk']
                            fields = game_data['fields'].copy()

                            developers = fields.pop('developers', [])
                            publishers = fields.pop('publishers', [])
                            genres = fields.pop('genres', [])
                            platforms = fields.pop('platforms', [])
                            keywords = fields.pop('keywords', [])
                            engines = fields.pop('engines', [])
                            themes = fields.pop('themes', [])
                            player_perspectives = fields.pop('player_perspectives', [])
                            game_modes = fields.pop('game_modes', [])
                            series = fields.pop('series', [])
                            screenshots = fields.pop('screenshots', [])

                            parent_game_id = fields.get('parent_game')
                            version_parent_id = fields.get('version_parent')

                            # Подготавливаем все поля для SQL INSERT
                            igdb_id = fields.get('igdb_id', 0)
                            name = sql_value(fields.get('name', ''), True)
                            summary = sql_value(fields.get('summary'), True)
                            game_type = sql_value(fields.get('game_type'))
                            version_title = sql_value(fields.get('version_title'), True)
                            rawg_description = sql_value(fields.get('rawg_description'), True)
                            storyline = sql_value(fields.get('storyline'), True)
                            rating = sql_value(fields.get('rating'))
                            rating_count = fields.get('rating_count', 0)
                            first_release_date = sql_value(fields.get('first_release_date'), True)
                            series_order = sql_value(fields.get('series_order'))
                            cover_url = sql_value(fields.get('cover_url'), True)
                            wiki_description = sql_value(fields.get('wiki_description'), True)

                            # Добавляем date_added и updated_at с NOW()
                            date_added = sql_value(fields.get('date_added'), True)
                            if date_added == 'NULL':
                                date_added = 'NOW()'
                            updated_at = sql_value(fields.get('updated_at'), True)
                            if updated_at == 'NULL':
                                updated_at = 'NOW()'

                            import json as json_module
                            developer_ids = sql_value(json_module.dumps(developers), True)
                            game_mode_ids = sql_value(json_module.dumps(game_modes), True)
                            genre_ids = sql_value(json_module.dumps(genres), True)
                            keyword_ids = sql_value(json_module.dumps(keywords), True)
                            perspective_ids = sql_value(json_module.dumps(player_perspectives), True)
                            theme_ids = sql_value(json_module.dumps(themes), True)
                            engine_ids = sql_value(json_module.dumps(engines), True)

                            game_values.append(f"""({game_id}, {igdb_id}, {name}, {summary}, {game_type}, 
                                {version_title}, {rawg_description}, {storyline}, {rating}, {rating_count},
                                {first_release_date}, {series_order}, {cover_url}, {wiki_description},
                                {developer_ids}, {game_mode_ids}, {genre_ids}, {keyword_ids},
                                {perspective_ids}, {theme_ids}, {engine_ids}, {date_added}, {updated_at})""")

                            game_m2m_data.append({
                                'game_id': game_id,
                                'developers': developers,
                                'publishers': publishers,
                                'genres': genres,
                                'platforms': platforms,
                                'keywords': keywords,
                                'engines': engines,
                                'themes': themes,
                                'player_perspectives': player_perspectives,
                                'game_modes': game_modes,
                                'series': series,
                                'screenshots': screenshots,
                            })

                            game_parent_refs.append({
                                'game_id': game_id,
                                'parent_game_id': parent_game_id,
                                'version_parent_id': version_parent_id,
                            })

                            pbar.update(1)

                    # Raw SQL INSERT для Game
                    if game_values:
                        batch_size = 2000
                        inserted = 0
                        with tqdm(total=len(game_values), desc="  ⏳ Saving games", unit="rec",
                                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
                            with connection.cursor() as cursor:
                                for i in range(0, len(game_values), batch_size):
                                    batch = game_values[i:i + batch_size]
                                    sql = f"""
                                        INSERT INTO games_game (id, igdb_id, name, summary, game_type, version_title,
                                            rawg_description, storyline, rating, rating_count, first_release_date,
                                            series_order, cover_url, wiki_description, developer_ids, game_mode_ids,
                                            genre_ids, keyword_ids, perspective_ids, theme_ids, engine_ids,
                                            date_added, updated_at)
                                        VALUES {','.join(batch)}
                                        ON CONFLICT (id) DO NOTHING
                                    """
                                    cursor.execute(sql)
                                    inserted += cursor.rowcount
                                    pbar.update(len(batch))
                        print(f"    ✅ Games saved: {inserted}/{len(games_data)}")
                        total_objects += inserted

                    # Обновляем parent_game и version_parent через raw SQL
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT id FROM games_game")
                        existing_ids = set(row[0] for row in cursor.fetchall())

                    parent_updates = []
                    for ref in game_parent_refs:
                        if ref['parent_game_id'] or ref['version_parent_id']:
                            valid_parent = True
                            if ref['parent_game_id'] and ref['parent_game_id'] not in existing_ids:
                                valid_parent = False
                            if ref['version_parent_id'] and ref['version_parent_id'] not in existing_ids:
                                valid_parent = False

                            if valid_parent:
                                parent_updates.append(ref)

                    if parent_updates:
                        updated = 0
                        with tqdm(total=len(parent_updates), desc="  ⏳ Updating parents", unit="rec",
                                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}') as pbar:
                            with connection.cursor() as cursor:
                                for ref in parent_updates:
                                    updates = []
                                    if ref['parent_game_id'] and ref['parent_game_id'] in existing_ids:
                                        updates.append(f"parent_game_id = {ref['parent_game_id']}")
                                    if ref['version_parent_id'] and ref['version_parent_id'] in existing_ids:
                                        updates.append(f"version_parent_id = {ref['version_parent_id']}")
                                    if updates:
                                        sql = f"""
                                            UPDATE games_game 
                                            SET {', '.join(updates)}
                                            WHERE id = {ref['game_id']}
                                        """
                                        cursor.execute(sql)
                                        updated += cursor.rowcount
                                    pbar.update(1)
                        print(f"    ✅ Parents updated: {updated}")

                    # ManyToMany связи через raw SQL с оптимизацией для keywords
                    print("  🔗 Creating ManyToMany relationships...")

                    with connection.cursor() as cursor:
                        # Обрабатываем все связи кроме keywords с увеличенным batch_size
                        m2m_mappings = [
                            ('developers', 'games_game_developers', 'company_id'),
                            ('publishers', 'games_game_publishers', 'company_id'),
                            ('genres', 'games_game_genres', 'genre_id'),
                            ('platforms', 'games_game_platforms', 'platform_id'),
                            ('engines', 'games_game_engines', 'gameengine_id'),
                            ('themes', 'games_game_themes', 'theme_id'),
                            ('player_perspectives', 'games_game_player_perspectives', 'playerperspective_id'),
                            ('game_modes', 'games_game_game_modes', 'gamemode_id'),
                            ('series', 'games_game_series', 'series_id'),
                        ]

                        for field_name, table_name, column_name in m2m_mappings:
                            values = []
                            for m2m in game_m2m_data:
                                game_id = m2m['game_id']
                                for related_id in m2m[field_name]:
                                    values.append(f"({game_id}, {related_id})")

                            if values:
                                print(f"    ⏳ Creating {field_name} relations: {len(values)} records...")
                                batch_size = 20000
                                inserted = 0
                                with tqdm(total=len(values), desc=f"    ⏳ {field_name}", unit="rel",
                                          bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}') as pbar:
                                    for i in range(0, len(values), batch_size):
                                        batch = values[i:i + batch_size]
                                        sql = f"""
                                            INSERT INTO {table_name} (game_id, {column_name})
                                            VALUES {','.join(batch)}
                                            ON CONFLICT (game_id, {column_name}) DO NOTHING
                                        """
                                        cursor.execute(sql)
                                        inserted += cursor.rowcount
                                        total_objects += cursor.rowcount
                                        pbar.update(len(batch))
                                print(f"      ✅ Inserted: {inserted} relations")

                        # Обрабатываем keywords отдельно с максимальной оптимизацией
                        total_keywords = sum(len(m2m['keywords']) for m2m in game_m2m_data)
                        if total_keywords > 0:
                            print(f"    ⏳ Creating keywords relations: {total_keywords} records...")

                            # Используем генератор для экономии памяти
                            def keyword_values_generator():
                                for m2m in game_m2m_data:
                                    game_id = m2m['game_id']
                                    for keyword_id in m2m['keywords']:
                                        yield f"({game_id}, {keyword_id})"

                            # Вставляем очень большими батчами по 100000 записей
                            batch_size = 100000
                            batch = []
                            inserted = 0

                            with tqdm(total=total_keywords, desc="    ⏳ keywords", unit="rel",
                                      bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
                                for value in keyword_values_generator():
                                    batch.append(value)
                                    if len(batch) >= batch_size:
                                        sql = f"""
                                            INSERT INTO games_game_keywords (game_id, keyword_id)
                                            VALUES {','.join(batch)}
                                            ON CONFLICT (game_id, keyword_id) DO NOTHING
                                        """
                                        cursor.execute(sql)
                                        inserted += cursor.rowcount
                                        total_objects += cursor.rowcount
                                        pbar.update(len(batch))
                                        batch = []

                                # Вставляем остаток
                                if batch:
                                    sql = f"""
                                        INSERT INTO games_game_keywords (game_id, keyword_id)
                                        VALUES {','.join(batch)}
                                        ON CONFLICT (game_id, keyword_id) DO NOTHING
                                    """
                                    cursor.execute(sql)
                                    inserted += cursor.rowcount
                                    total_objects += cursor.rowcount
                                    pbar.update(len(batch))

                            print(f"      ✅ Inserted: {inserted} keyword relations")

                    # ИМПОРТ SCREENSHOTS через raw SQL
                    screenshot_values = []
                    for m2m in game_m2m_data:
                        game_id = m2m['game_id']
                        for screenshot_data in m2m['screenshots']:
                            if isinstance(screenshot_data, dict):
                                url = screenshot_data.get('url', '').replace("'", "''")
                                w = screenshot_data.get('w', 0)
                                h = screenshot_data.get('h', 0)
                                primary = str(screenshot_data.get('primary', False)).lower()
                                screenshot_values.append(f"({game_id}, '{url}', {w}, {h}, {primary})")

                    if screenshot_values:
                        print(f"    ⏳ Creating screenshots: {len(screenshot_values)} records...")
                        batch_size = 10000
                        inserted = 0
                        with tqdm(total=len(screenshot_values), desc="    ⏳ Screenshots", unit="rec",
                                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}') as pbar:
                            with connection.cursor() as cursor:
                                for i in range(0, len(screenshot_values), batch_size):
                                    batch = screenshot_values[i:i + batch_size]
                                    sql = f"""
                                        INSERT INTO games_screenshot (game_id, url, w, h, "primary")
                                        VALUES {','.join(batch)}
                                        ON CONFLICT DO NOTHING
                                    """
                                    cursor.execute(sql)
                                    inserted += cursor.rowcount
                                    pbar.update(len(batch))
                        print(f"      ✅ Inserted: {inserted} screenshots")
                        total_objects += inserted

                    del game_values
                    del game_m2m_data
                    del game_parent_refs
                    gc.collect()

                with connection.cursor() as cursor:
                    cursor.execute('SET CONSTRAINTS ALL IMMEDIATE')
                    cursor.execute('SET synchronous_commit TO ON')

                import_time = time.time() - import_start_time
                print(f"\n{'=' * 50}")
                print(f"✅ ИМПОРТ ЗАВЕРШЁН за {import_time:.1f} сек")
                print(f"{'=' * 50}")
                print(f"📊 Сохранено: {total_objects} записей")
                print(f"⚡ Скорость: {total_objects / import_time:.0f} rec/sec")

                User = get_user_model()
                if User.objects.count() == 0:
                    print("👤 Creating default superuser...")
                    User.objects.create_superuser('admin', 'admin@localhost.com', 'admin')
                    print("⚠️ Default superuser created: admin / admin")

            except Exception as e:
                print(f"⚠️ Import error: {e}")
                import traceback
                traceback.print_exc()
        else:
            print("⚠️ No data.json found. Database will be empty.")


def main():
    """Main entry point for the desktop application."""
    from datetime import datetime
    import sys
    from pathlib import Path

    # Настройка логирования в файл
    if getattr(sys, 'frozen', False):
        exe_dir = Path(sys.executable).parent
    else:
        exe_dir = Path.cwd()

    logs_dir = exe_dir / 'logs'
    logs_dir.mkdir(exist_ok=True)

    log_filename = logs_dir / f'gamespeek_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

    # Открываем файл лога для записи
    log_file = open(log_filename, 'w', encoding='utf-8', buffering=1)

    # Перенаправляем stdout и stderr в файл и консоль
    class TeeLogger:
        def __init__(self, log_file, console_stream, is_error=False):
            self.log_file = log_file
            self.console_stream = console_stream
            self.is_error = is_error

        def write(self, message):
            if message.strip():
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                log_line = f"{timestamp} - {'ERROR' if self.is_error else 'INFO'} - {message}"
                self.log_file.write(log_line + '\n')
                self.log_file.flush()
            if self.console_stream:
                self.console_stream.write(message)
                self.console_stream.flush()

        def flush(self):
            self.log_file.flush()
            if self.console_stream:
                self.console_stream.flush()

    # Сохраняем оригинальные потоки
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    # Перенаправляем stdout и stderr
    sys.stdout = TeeLogger(log_file, sys.__stdout__, is_error=False)
    sys.stderr = TeeLogger(log_file, sys.__stderr__, is_error=True)

    try:
        print("=" * 50)
        print("🎮 gamespeek Desktop Launcher")
        print("=" * 50)
        print(f"📝 Log file: {log_filename}")

        setup_environment()

        database_url = start_postgresql()
        os.environ['DATABASE_URL'] = database_url

        import django
        django.setup()

        from django.conf import settings

        if getattr(sys, 'frozen', False):
            exe_dir = Path(sys.executable).parent
            static_root = exe_dir / 'staticfiles'

            if static_root.exists():
                settings.STATIC_ROOT = str(static_root)
                settings.STATIC_URL = '/static/'

                print(f"✅ Static files found at: {static_root}")

                games_css = static_root / 'games' / 'css'
                if games_css.exists():
                    css_files = list(games_css.glob('*.css'))
                    print(f"   - Found {len(css_files)} CSS files in games/css")
                    for css_file in css_files:
                        print(f"     * {css_file.name}")

                admin_static = static_root / 'admin'
                if admin_static.exists():
                    print(f"   - Admin static files: OK")

                style_css = games_css / 'style.css'
                if style_css.exists():
                    print(f"   - style.css: OK ({style_css.stat().st_size} bytes)")
                else:
                    print(f"   - style.css: NOT FOUND")
            else:
                print(f"⚠️ Static files not found at: {static_root}")
                print(f"   Contents of {exe_dir}:")
                for item in exe_dir.iterdir():
                    if item.is_dir():
                        print(f"     📁 {item.name}/")
                    else:
                        print(f"     📄 {item.name}")

        run_migrations_once()

        print("\n" + "=" * 50)
        print("🌐 SERVER RUNNING")
        print("Open your browser and go to: http://127.0.0.1:8000")
        print("Press Ctrl+C to stop the server")
        print("=" * 50 + "\n")
        print(f"📝 All output is being logged to: {log_filename}")

        from django.core.management import execute_from_command_line
        execute_from_command_line(['manage.py', 'runserver', '--noreload', '127.0.0.1:8000'])

    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Восстанавливаем оригинальные потоки
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        log_file.close()
        print(f"📝 Log saved to: {log_filename}")


if __name__ == '__main__':
    main()