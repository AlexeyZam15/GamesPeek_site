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


def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown on window close."""
    import signal
    import sys

    def signal_handler(signum, frame):
        """Handle SIGTERM and SIGINT signals."""
        print("\n🛑 Received shutdown signal. Cleaning up...")
        cleanup_resources()
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Termination signal

    # For Windows, also handle console close event
    if sys.platform == 'win32':
        try:
            import win32api
            import win32con

            def console_ctrl_handler(ctrl_type):
                """Handle Windows console close event."""
                if ctrl_type == 0:  # CTRL_C_EVENT
                    print("\n🛑 Received Ctrl+C. Cleaning up...")
                elif ctrl_type == 2:  # CTRL_CLOSE_EVENT (window X button)
                    print("\n🛑 Window closed. Cleaning up PostgreSQL...")
                cleanup_resources()
                return True

            win32api.SetConsoleCtrlHandler(console_ctrl_handler, True)
        except ImportError:
            print("[WARNING] pywin32 not installed. Window X button may not be handled.")
            print("         Install: pip install pywin32")


def cleanup_resources():
    """Clean up PostgreSQL server and other resources."""
    global _postgresql_server

    if _postgresql_server:
        try:
            print("  Stopping PostgreSQL server...")
            _postgresql_server.cleanup()
            print("  ✅ PostgreSQL stopped")
        except Exception as e:
            print(f"  ⚠️ Error stopping PostgreSQL: {e}")

    # Close log file if exists
    if hasattr(sys.stdout, 'log_file'):
        try:
            sys.stdout.log_file.close()
        except:
            pass


def start_postgresql():
    """
    Start embedded PostgreSQL server.
    Data persists in user's AppData folder between launches.
    Returns database URL after successful connection.
    """
    global _postgresql_server

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

    Path(data_dir).mkdir(parents=True, exist_ok=True)

    print(f"📁 Database stored at: {data_dir}")

    # Kill any existing PostgreSQL processes using this data directory
    def kill_orphaned_postgresql():
        """Kill any PostgreSQL processes that might be using our data directory."""
        import subprocess
        import psutil

        print("  Checking for orphaned PostgreSQL processes...")
        killed_count = 0

        if sys.platform == 'win32':
            try:
                for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                    try:
                        if proc.info['name'] and 'postgres' in proc.info['name'].lower():
                            cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                            if data_dir in cmdline:
                                print(f"    Killing orphaned PostgreSQL process PID {proc.info['pid']}")
                                proc.kill()
                                killed_count += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
            except Exception as e:
                print(f"    Warning: Could not check processes: {e}")
        else:
            try:
                result = subprocess.run(
                    ['pgrep', '-f', f'postgres.*{data_dir}'],
                    capture_output=True,
                    text=True
                )
                if result.stdout:
                    pids = result.stdout.strip().split('\n')
                    for pid in pids:
                        if pid:
                            print(f"    Killing orphaned PostgreSQL process PID {pid}")
                            os.kill(int(pid), 9)
                            killed_count += 1
            except Exception as e:
                print(f"    Warning: Could not check processes: {e}")

        if killed_count > 0:
            print(f"    Killed {killed_count} orphaned processes")
            time.sleep(2)
        else:
            print("    No orphaned processes found")

    def cleanup_port(port):
        """Kill process using specific port."""
        import subprocess
        import psutil

        if sys.platform == 'win32':
            try:
                result = subprocess.run(
                    f'netstat -ano | findstr :{port} | findstr LISTENING',
                    shell=True,
                    capture_output=True,
                    text=True
                )
                if result.stdout:
                    lines = result.stdout.strip().split('\n')
                    for line in lines:
                        parts = line.split()
                        if len(parts) >= 5:
                            pid = parts[4]
                            try:
                                proc = psutil.Process(int(pid))
                                if 'postgres' in proc.name().lower():
                                    print(f"    Killing process on port {port} (PID {pid})")
                                    proc.kill()
                            except:
                                pass
            except Exception as e:
                print(f"    Warning: Could not cleanup port {port}: {e}")

    kill_orphaned_postgresql()

    import socket
    import time

    def check_port(port):
        """Check if port is available."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            return sock.connect_ex(('127.0.0.1', port)) != 0

    def wait_for_postgres(port, timeout=30):
        """Wait for PostgreSQL to accept connections."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                import psycopg2
                conn = psycopg2.connect(
                    host='127.0.0.1',
                    port=port,
                    user='postgres',
                    connect_timeout=1,
                    sslmode='disable'
                )
                conn.close()
                return True
            except:
                time.sleep(0.5)
        return False

    max_retries = 3
    server = None
    database_url = None

    for attempt in range(max_retries):
        try:
            print(f"  Attempt {attempt + 1}/{max_retries} to start PostgreSQL...")

            import re
            test_url = f"postgres://user@localhost:7937"
            port_match = re.search(r':(\d+)', test_url)
            test_port = int(port_match.group(1)) if port_match else 7937
            cleanup_port(test_port)

            server = pgembed.get_server(data_dir, cleanup_mode='stop')
            _postgresql_server = server  # Store for cleanup

            database_url = server.get_uri()
            port_match = re.search(r':(\d+)', database_url)
            port = int(port_match.group(1)) if port_match else 7937

            if wait_for_postgres(port, timeout=30):
                print(f"✅ PostgreSQL started successfully on port {port}")

                def cleanup_server():
                    global _postgresql_server
                    if _postgresql_server:
                        try:
                            print("  Cleaning up PostgreSQL server...")
                            _postgresql_server.cleanup()
                            time.sleep(1)
                            kill_orphaned_postgresql()
                            _postgresql_server = None
                        except:
                            pass

                atexit.register(cleanup_server)
                break
            else:
                print(f"  Server started but not responding, retrying...")
                if server:
                    try:
                        server.cleanup()
                        _postgresql_server = None
                    except:
                        pass
                time.sleep(2)
                continue

        except Exception as e:
            print(f"  Attempt failed: {e}")
            if server:
                try:
                    server.cleanup()
                    _postgresql_server = None
                except:
                    pass
            time.sleep(2)
            if attempt == max_retries - 1:
                print(f"❌ Failed to start PostgreSQL after {max_retries} attempts")
                sys.exit(1)

    # Fix database URL - convert to postgres:// and disable SSL
    if database_url:
        if database_url.startswith('postgresql://'):
            database_url = database_url.replace('postgresql://', 'postgres://')

        # Remove any existing sslmode parameter
        if 'sslmode=' in database_url:
            import re
            database_url = re.sub(r'[?&]sslmode=[^&]*', '', database_url)

        # Add sslmode=disable
        if '?' in database_url:
            database_url += '&sslmode=disable'
        else:
            database_url += '?sslmode=disable'

        # Also set environment variable for Django
        os.environ['PGSSLMODE'] = 'disable'

    print("✅ PostgreSQL ready")
    return database_url


def check_database_state():
    """Check if database tables and data exist."""
    from django.db import connection

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

    return has_games_table, has_games_data


def apply_database_optimizations():
    """Apply PostgreSQL optimizations and fix array comparison issues."""
    from django.db import connection

    with connection.cursor() as cursor:
        cursor.execute('SET CONSTRAINTS ALL DEFERRED')
        cursor.execute('SET synchronous_commit TO OFF')
        cursor.execute('SET maintenance_work_mem TO "1GB"')
        cursor.execute('SET work_mem TO "256MB"')

        # Check if function exists before creating
        cursor.execute("""
                       SELECT EXISTS (SELECT 1
                                      FROM pg_proc
                                      WHERE proname = 'safe_array_equals'
                                        AND pronargs = 2);
                       """)

        function_exists = cursor.fetchone()[0]

        if not function_exists:
            # Create function without DO block - simpler syntax
            cursor.execute("""
                           CREATE FUNCTION safe_array_equals(text, integer [])
                               RETURNS boolean AS
                               $$
                           SELECT FALSE;
                           $$
                           LANGUAGE sql
                IMMUTABLE;
                           """)


def restore_database_settings():
    """Restore PostgreSQL settings after import."""
    from django.db import connection
    from django.db import transaction

    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute('SET CONSTRAINTS ALL IMMEDIATE')
            cursor.execute('SET synchronous_commit TO ON')


def import_independent_model(data_key, model_class, data, results):
    """Import independent model data in parallel."""
    if data_key in data and data[data_key]:
        objects_data = data[data_key]
        objects_to_create = []
        for item in objects_data:
            fields = item['fields'].copy()
            objects_to_create.append(model_class(id=item['pk'], **fields))

        batch_size = 10000
        saved_count = 0
        for i in range(0, len(objects_to_create), batch_size):
            batch = objects_to_create[i:i + batch_size]
            created = model_class.objects.bulk_create(batch, ignore_conflicts=True)
            saved_count += len(created)
        results[data_key] = saved_count


def import_keyword_category(data):
    """Import KeywordCategory data."""
    from games import models as game_models
    from tqdm import tqdm
    import gc

    if 'KeywordCategory' not in data or not data['KeywordCategory']:
        return 0

    objects_data = data['KeywordCategory']
    print(f"\n  ⚡ Importing KeywordCategory: {len(objects_data)} records...")

    objects_to_create = []
    for item in objects_data:
        fields = item['fields'].copy()
        objects_to_create.append(game_models.KeywordCategory(id=item['pk'], **fields))

    batch_size = 5000
    saved_count = 0
    total_objects = 0

    with tqdm(total=len(objects_to_create), desc="  ⏳ Saving", unit="rec",
              bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}') as pbar:
        for i in range(0, len(objects_to_create), batch_size):
            batch = objects_to_create[i:i + batch_size]
            created = game_models.KeywordCategory.objects.bulk_create(batch, ignore_conflicts=True)
            saved_count += len(created)
            total_objects += len(created)
            pbar.update(len(batch))

    print(f"    ✅ Saved: {saved_count}/{len(objects_data)} records")
    del objects_to_create
    gc.collect()
    return total_objects


def import_keywords(data):
    """Import Keyword data using raw SQL for speed."""
    from django.db import connection
    from tqdm import tqdm
    import gc

    if 'Keyword' not in data or not data['Keyword']:
        return 0

    objects_data = data['Keyword']
    print(f"\n  ⚡ Importing Keyword: {len(objects_data)} records...")

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

    if not values_list:
        return 0

    batch_size = 10000
    inserted = 0

    with tqdm(total=len(values_list), desc="  ⏳ Saving", unit="rec",
              bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}') as pbar:
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
    del values_list
    gc.collect()
    return inserted


def prepare_game_data(games_data, screenshots_data=None):
    """Prepare game data for bulk insert with optional screenshots."""
    game_values = []
    game_m2m_data = []
    game_parent_refs = []

    # Создаем словарь скриншотов по game_id если передан
    screenshots_by_game = {}
    if screenshots_data:
        for item in screenshots_data:
            fields = item['fields']
            game_id = fields.get('game')
            if game_id:
                if game_id not in screenshots_by_game:
                    screenshots_by_game[game_id] = []
                screenshots_by_game[game_id].append({
                    'url': fields.get('url', ''),
                    'w': fields.get('w', 0),
                    'h': fields.get('h', 0),
                    'primary': fields.get('primary', False),
                })

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

    games_data_sorted = sorted(
        games_data,
        key=lambda x: (
            (x['fields'].get('parent_game') is not None) or (x['fields'].get('version_parent') is not None),
            x['fields'].get('parent_game') or 0,
            x['fields'].get('version_parent') or 0
        )
    )

    from tqdm import tqdm

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

            # Берем скриншоты из словаря, загруженного из Screenshot модели
            screenshots = screenshots_by_game.get(game_id, [])

            parent_game_id = fields.get('parent_game')
            version_parent_id = fields.get('version_parent')

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

    return game_values, game_m2m_data, game_parent_refs


def insert_games(game_values, games_data):
    """Insert games using raw SQL."""
    from django.db import connection
    from tqdm import tqdm

    if not game_values:
        return 0

    batch_size = 2000
    inserted = 0

    with tqdm(total=len(game_values), desc="  ⏳ Saving games", unit="rec",
              bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}') as pbar:
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
    return inserted


def update_game_parents(game_parent_refs):
    """Update parent_game and version_parent references."""
    from django.db import connection
    from tqdm import tqdm

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

    if not parent_updates:
        return 0

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
    return updated


def insert_m2m_batch(table_name, column_name, values_batch):
    """Insert one batch of ManyToMany relationships."""
    from django.db import connections

    with connections['default'].cursor() as cursor:
        sql = f"""
            INSERT INTO {table_name} (game_id, {column_name})
            VALUES {','.join(values_batch)}
            ON CONFLICT (game_id, {column_name}) DO NOTHING
        """
        cursor.execute(sql)
        return cursor.rowcount


def process_m2m_parallel(field_name, table_name, column_name, game_m2m_data):
    """Process one ManyToMany relationship in parallel."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    values = []
    for m2m in game_m2m_data:
        game_id = m2m['game_id']
        for related_id in m2m[field_name]:
            values.append(f"({game_id}, {related_id})")

    if not values:
        return 0

    print(f"    ⏳ Creating {field_name} relations: {len(values)} records...")

    batch_size = 20000
    batches = [values[i:i + batch_size] for i in range(0, len(values), batch_size)]

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for batch in batches:
            future = executor.submit(insert_m2m_batch, table_name, column_name, batch)
            futures.append(future)

        total_inserted = 0
        for future in as_completed(futures):
            total_inserted += future.result()

    print(f"      ✅ Inserted: {total_inserted} {field_name} relations")
    return total_inserted


def insert_keywords_relations(game_m2m_data):
    """Insert keyword relations using MAXIMUM speed single-threaded batch insert."""
    from django.db import connection
    from tqdm import tqdm

    # Собираем уникальные пары через set comprehension
    unique_pairs = {(m2m['game_id'], keyword_id)
                    for m2m in game_m2m_data
                    for keyword_id in m2m['keywords']}

    if not unique_pairs:
        return 0

    print(f"    ⏳ Creating keywords relations: {len(unique_pairs)} unique records...")

    # Генератор значений
    values_gen = (f"({game_id}, {keyword_id})" for game_id, keyword_id in unique_pairs)

    batch_size = 100000
    inserted = 0

    with connection.cursor() as cursor:
        # Отключаем проверки ограничений для скорости
        cursor.execute("SET session_replication_role = replica;")

        batch = []
        batch_count = 0

        with tqdm(total=len(unique_pairs), desc="    ⏳ Keywords insert", unit="rec",
                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}') as pbar:

            for value in values_gen:
                batch.append(value)
                batch_count += 1

                if len(batch) >= batch_size:
                    sql = f"""
                        INSERT INTO games_game_keywords (game_id, keyword_id)
                        VALUES {','.join(batch)}
                        ON CONFLICT (game_id, keyword_id) DO NOTHING
                    """
                    cursor.execute(sql)
                    inserted += cursor.rowcount
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
                pbar.update(len(batch))

        # Включаем проверки ограничений обратно
        cursor.execute("SET session_replication_role = DEFAULT;")

    print(f"      ✅ Inserted: {inserted} keyword relations")
    return inserted


def insert_screenshots(game_m2m_data):
    """Insert screenshot data using MAXIMUM speed batch insert."""
    from django.db import connection
    from tqdm import tqdm

    # Оптимизация 1: Используем set comprehension для уникальности
    unique_screenshots = set()
    for m2m in game_m2m_data:
        for screenshot_data in m2m['screenshots']:
            if isinstance(screenshot_data, dict):
                url = screenshot_data.get('url', '')
                if url:
                    w = screenshot_data.get('w', 0)
                    h = screenshot_data.get('h', 0)
                    primary = screenshot_data.get('primary', False)
                    unique_screenshots.add((m2m['game_id'], url, w, h, primary))

    if not unique_screenshots:
        print("    ⏳ No screenshots to insert")
        return 0

    print(f"    ⏳ Creating screenshots: {len(unique_screenshots)} unique records...")

    # Готовим VALUES
    values = []
    for game_id, url, w, h, primary in unique_screenshots:
        escaped_url = url.replace("'", "''")
        primary_str = 'TRUE' if primary else 'FALSE'
        values.append(f"({game_id}, '{escaped_url}', {w}, {h}, {primary_str})")

    batch_size = 50000
    inserted = 0

    with connection.cursor() as cursor:
        # Отключаем проверки ограничений
        cursor.execute("SET session_replication_role = replica;")

        with tqdm(total=len(values), desc="    ⏳ Screenshots insert", unit="rec",
                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}') as pbar:
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                sql = f"""
                    INSERT INTO games_screenshot (game_id, url, w, h, "primary")
                    VALUES {','.join(batch)}
                    ON CONFLICT DO NOTHING
                """
                cursor.execute(sql)
                inserted += cursor.rowcount
                pbar.update(len(batch))

        # Включаем проверки обратно
        cursor.execute("SET session_replication_role = DEFAULT;")

    print(f"      ✅ Inserted: {inserted} screenshots")
    return inserted


def import_independent_models_parallel(data):
    """Import all independent models in parallel."""
    from games import models as game_models
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import gc

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

    print("\n  ⚡ Importing independent models in parallel...")
    results = {}

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for data_key, model_class in independent_models.items():
            future = executor.submit(import_independent_model, data_key, model_class, data, results)
            futures.append(future)

        for future in as_completed(futures):
            future.result()

    total_objects = 0
    for data_key, count in results.items():
        if count:
            print(f"    ✅ {data_key}: {count} records")
            total_objects += count

    gc.collect()
    return total_objects


def import_all_m2m_relationships(game_m2m_data):
    """Import all ManyToMany relationships sequentially to avoid deadlocks."""

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

    total_objects = 0

    for field_name, table_name, column_name in m2m_mappings:
        total_objects += process_m2m_sequential(field_name, table_name, column_name, game_m2m_data)

    return total_objects


def process_m2m_sequential(field_name, table_name, column_name, game_m2m_data):
    """Process one ManyToMany relationship using MAXIMUM speed single-threaded batch insert."""
    from django.db import connection
    from tqdm import tqdm

    # Собираем уникальные пары через set comprehension
    unique_pairs = {(m2m['game_id'], related_id)
                    for m2m in game_m2m_data
                    for related_id in m2m[field_name]}

    if not unique_pairs:
        return 0

    print(f"    ⏳ Creating {field_name} relations: {len(unique_pairs)} unique records...")

    # Генератор значений
    values_gen = (f"({game_id}, {related_id})" for game_id, related_id in unique_pairs)

    batch_size = 100000
    inserted = 0

    with connection.cursor() as cursor:
        # Отключаем проверки ограничений для скорости
        cursor.execute("SET session_replication_role = replica;")

        batch = []
        with tqdm(total=len(unique_pairs), desc=f"    ⏳ {field_name} insert", unit="rec",
                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}') as pbar:

            for value in values_gen:
                batch.append(value)

                if len(batch) >= batch_size:
                    sql = f"""
                        INSERT INTO {table_name} (game_id, {column_name})
                        VALUES {','.join(batch)}
                        ON CONFLICT (game_id, {column_name}) DO NOTHING
                    """
                    cursor.execute(sql)
                    inserted += cursor.rowcount
                    pbar.update(len(batch))
                    batch = []

            # Вставляем остаток
            if batch:
                sql = f"""
                    INSERT INTO {table_name} (game_id, {column_name})
                    VALUES {','.join(batch)}
                    ON CONFLICT (game_id, {column_name}) DO NOTHING
                """
                cursor.execute(sql)
                inserted += cursor.rowcount
                pbar.update(len(batch))

        # Включаем проверки ограничений обратно
        cursor.execute("SET session_replication_role = DEFAULT;")

    print(f"      ✅ Inserted: {inserted} {field_name} relations")
    return inserted

def create_default_superuser():
    """Create default superuser if none exists."""
    from django.contrib.auth import get_user_model

    User = get_user_model()
    if User.objects.count() == 0:
        print("👤 Creating default superuser...")
        User.objects.create_superuser('admin', 'admin@localhost.com', 'admin')
        print("⚠️ Default superuser created: admin / admin")


def run_migrations_once():
    """
    Run desktop-specific migration to create ALL tables, then import data.
    Optimized for maximum speed using parallel processing and optimized queries.
    """
    from django.core.management import call_command
    from django.db import transaction
    from django.db import connection
    import json
    from tqdm import tqdm
    import time
    from pathlib import Path
    import sys
    import gc

    print("📦 Applying Django core migrations...")
    call_command('migrate', interactive=False)
    print("✅ Core migrations completed")

    has_games_table, has_games_data = check_database_state()

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

    if not needs_import:
        return

    if getattr(sys, 'frozen', False):
        exe_dir = Path(sys.executable).parent
    else:
        exe_dir = Path.cwd()

    data_file = exe_dir / 'data.json'

    if not data_file.exists():
        print("⚠️ No data.json found. Database will be empty.")
        return

    file_size_mb = data_file.stat().st_size / 1024 / 1024
    print(f"📦 Found data.json ({file_size_mb:.1f} MB)")
    print("📖 Loading JSON file...")

    try:
        with tqdm(total=100, desc="  ⏳ Loading JSON", unit="%",
                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}') as pbar:
            with open(data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            pbar.update(100)

        print(f"✅ JSON loaded successfully")
        print(f"📊 Found {len(data)} model types in fixture")

        # Apply optimizations in transaction
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute('SET CONSTRAINTS ALL DEFERRED')
                cursor.execute('SET synchronous_commit TO OFF')
                cursor.execute('SET maintenance_work_mem TO "1GB"')
                cursor.execute('SET work_mem TO "256MB"')

                # Create safe array comparison function
                cursor.execute("""
                               CREATE
                               OR REPLACE FUNCTION safe_array_equals(text, integer[])
                    RETURNS boolean AS
                    $$
                               SELECT FALSE;
                               $$
                               LANGUAGE sql
                    IMMUTABLE;
                               """)

        total_objects = 0
        import_start_time = time.time()

        total_objects += import_keyword_category(data)
        total_objects += import_independent_models_parallel(data)
        total_objects += import_keywords(data)

        if 'Game' in data and data['Game']:
            games_data = data['Game']
            screenshots_data = data.get('Screenshot', [])

            print(f"\n  🚀 Importing Game: {len(games_data)} records...")
            if screenshots_data:
                print(f"  📸 Found {len(screenshots_data)} screenshots to attach...")

            game_values, game_m2m_data, game_parent_refs = prepare_game_data(games_data, screenshots_data)
            total_objects += insert_games(game_values, games_data)
            total_objects += update_game_parents(game_parent_refs)

            print("  🔗 Creating ManyToMany relationships...")
            total_objects += import_all_m2m_relationships(game_m2m_data)
            total_objects += insert_keywords_relations(game_m2m_data)
            total_objects += insert_screenshots(game_m2m_data)

            del game_values
            del game_m2m_data
            del game_parent_refs
            gc.collect()

        # Restore settings
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute('SET CONSTRAINTS ALL IMMEDIATE')
                cursor.execute('SET synchronous_commit TO ON')

        import_time = time.time() - import_start_time
        print(f"\n{'=' * 50}")
        print(f"✅ ИМПОРТ ЗАВЕРШЁН за {import_time:.1f} сек")
        print(f"{'=' * 50}")
        print(f"📊 Сохранено: {total_objects} записей")
        print(f"⚡ Скорость: {total_objects / import_time:.0f} rec/sec")

        create_default_superuser()

    except Exception as e:
        print(f"⚠️ Import error: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Main entry point for the desktop application."""
    from datetime import datetime
    import sys
    from pathlib import Path

    # Setup signal handlers for graceful shutdown
    setup_signal_handlers()

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

    # Store log file reference for cleanup
    sys.stdout.log_file = log_file
    sys.stderr.log_file = log_file

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
        print("Press Ctrl+C or close the window to stop the server")
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
        # Cleanup resources
        cleanup_resources()
        # Восстанавливаем оригинальные потоки
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        log_file.close()
        print(f"📝 Log saved to: {log_filename}")


if __name__ == '__main__':
    main()