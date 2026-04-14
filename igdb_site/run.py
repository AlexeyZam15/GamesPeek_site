#!/usr/bin/env python

"""

Desktop launcher for Django + PostgreSQL.

"""

import os
import sys
import tkinter as tk
from tkinter import ttk
from pathlib import Path
import queue
import threading
import time
import webbrowser
import subprocess
import json
import gc
import shutil
import tempfile
import socket
import site
import signal
import atexit
import traceback
from datetime import datetime
import gzip
import re

# Тяжёлые импорты
import psutil
import psycopg2
import django
from django.db import connection
from django.db import transaction
from django.core.management import call_command
from django.conf import settings
from django.contrib.auth import get_user_model
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ============================================
# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
# ============================================

_postgresql_server = None
_postgresql_bin_path = None
_postgresql_port = None
_postgresql_process = None
_postgresql_data_dir = None
server_process = None


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


# ============================================
# КЛАСС TEE LOGGER
# ============================================

class TeeLogger:
    """Custom logger that writes to both file and terminal widget with color coding."""

    def __init__(self, log_file, console_stream, is_error=False, terminal_callback=None):
        self.log_file = log_file
        self.console_stream = console_stream
        self.is_error = is_error
        self.terminal_callback = terminal_callback

        self.tag_map = {
            '✅': 'SUCCESS',
            '❌': 'ERROR',
            '⚠️': 'WARNING',
            '📦': 'INFO',
            '📁': 'INFO',
            '📊': 'INFO',
            '🔗': 'INFO',
            '🚀': 'INFO',
            '👤': 'INFO',
            '🛑': 'WARNING',
            '⚡': 'DEBUG',
            '⏳': 'DEBUG',
        }

    def _get_tag_for_message(self, message):
        for symbol, tag in self.tag_map.items():
            if symbol in message:
                return tag

        msg_lower = message.lower()
        if 'error' in msg_lower or 'failed' in msg_lower or 'fatal' in msg_lower:
            return 'ERROR'
        elif 'warning' in msg_lower or '⚠️' in message:
            return 'WARNING'
        elif 'success' in msg_lower or '✅' in message or 'completed' in msg_lower:
            return 'SUCCESS'
        elif 'debug' in msg_lower or '⚡' in message:
            return 'DEBUG'
        elif 'time' in msg_lower or 'sec' in msg_lower or 'rec/sec' in msg_lower:
            return 'TIME'
        else:
            return 'INFO'

    def set_terminal_callback(self, callback):
        self.terminal_callback = callback

    def write(self, message):
        if not message or message.strip() == '':
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        level = 'ERROR' if self.is_error else 'INFO'
        log_line = f"{timestamp} - {level} - {message}"

        if self.log_file:
            self.log_file.write(log_line + '\n')
            self.log_file.flush()

        if self.console_stream:
            try:
                self.console_stream.write(message)
                self.console_stream.flush()
            except UnicodeEncodeError:
                cleaned = message.encode('ascii', errors='ignore').decode('ascii')
                self.console_stream.write(cleaned)
                self.console_stream.flush()

        if self.terminal_callback:
            tag = self._get_tag_for_message(message)
            self.terminal_callback(message, tag)

    def flush(self):
        if self.log_file:
            self.log_file.flush()
        if self.console_stream:
            self.console_stream.flush()


# ============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ POSTGRESQL
# ============================================

def get_postgres_bin_path(output_callback=None):
    """Get path to PostgreSQL binaries from within the bundled app."""
    if getattr(sys, 'frozen', False):
        exe_dir = Path(sys.executable).parent
        pg_path = exe_dir / 'PostgreSQL' / '18' / 'bin'

        if output_callback:
            output_callback(f"  Looking for PostgreSQL at: {pg_path}\n")

        if pg_path.exists():
            pg_ctl = pg_path / 'pg_ctl.exe'
            initdb = pg_path / 'initdb.exe'

            if pg_ctl.exists() and initdb.exists():
                if output_callback:
                    output_callback(f"  ✅ Found PostgreSQL at: {pg_path}\n")
                return str(pg_path)

        raise Exception(f"PostgreSQL not found at {pg_path}")
    else:
        source_dir = Path(__file__).parent
        pg_path = source_dir / 'PostgreSQL' / '18' / 'bin'

        if pg_path.exists():
            return str(pg_path)

        raise Exception("PostgreSQL not found")


def fix_timezonesets(data_dir):
    """Copy timezonesets from temp folder to permanent location."""
    target_dir = Path(data_dir) / 'timezonesets'

    if not target_dir.exists():
        target_dir.mkdir(parents=True, exist_ok=True)
        default_file = target_dir / 'Default'
        if not default_file.exists():
            default_file.write_text(
                '# Default timezone abbreviations\n'
                'DST    3:00:00\n'
                'GMT    0:00:00\n'
                'UTC    0:00:00\n'
                'Z      0:00:00\n'
                'EST   -5:00:00\n'
                'EDT   -4:00:00\n'
                'CST   -6:00:00\n'
                'CDT   -5:00:00\n'
                'MST   -7:00:00\n'
                'MDT   -6:00:00\n'
                'PST   -8:00:00\n'
                'PDT   -7:00:00\n'
            )
            print(f"    Created default timezone file")
        return True

    temp_dir = tempfile.gettempdir()
    for item in Path(temp_dir).glob('_MEI*'):
        pgembed_temp = item / 'pgembed' / 'pginstall' / 'share' / 'postgresql' / 'timezonesets'
        if pgembed_temp.exists():
            shutil.copytree(pgembed_temp, target_dir, dirs_exist_ok=True)
            print(f"  ✅ Copied timezonesets to {target_dir}")
            return True

    if getattr(sys, 'frozen', False):
        exe_dir = Path(sys.executable).parent
        pgembed_local = exe_dir / 'pgembed' / 'pginstall' / 'share' / 'postgresql' / 'timezonesets'
        if pgembed_local.exists():
            shutil.copytree(pgembed_local, target_dir, dirs_exist_ok=True)
            print(f"  ✅ Copied timezonesets from exe dir to {target_dir}")
            return True

    print(f"  ⚠️ timezonesets not found, using default")
    return True


def kill_our_postgresql_processes(data_dir):
    """Kill ONLY PostgreSQL processes using our data directory."""
    print("  Checking for our PostgreSQL processes...")
    killed_count = 0

    if sys.platform == 'win32':
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] and 'postgres' in proc.info['name'].lower():
                        cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                        if data_dir in cmdline or '-D' in cmdline and data_dir in cmdline:
                            print(f"    Killing our PostgreSQL process PID {proc.info['pid']}")
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
                        print(f"    Killing our PostgreSQL process PID {pid}")
                        os.kill(int(pid), 9)
                        killed_count += 1
        except Exception as e:
            print(f"    Warning: Could not check processes: {e}")

    if killed_count > 0:
        print(f"    Killed {killed_count} of our processes")
        time.sleep(1)
    else:
        print("    No our PostgreSQL processes found")


def cleanup_recovery_files(data_dir):
    """Remove recovery files and stale pid files from data directory."""
    recovery_conf = Path(data_dir) / 'recovery.conf'
    recovery_signal = Path(data_dir) / 'recovery.signal'
    standby_signal = Path(data_dir) / 'standby.signal'
    pid_file = Path(data_dir) / 'postmaster.pid'

    for file_path in [recovery_conf, recovery_signal, standby_signal, pid_file]:
        if file_path.exists():
            try:
                file_path.unlink()
                print(f"    Removed: {file_path.name}")
            except Exception as e:
                print(f"    Could not remove {file_path.name}: {e}")


def force_cleanup_postgresql(data_dir):
    """Агрессивная очистка всех следов PostgreSQL."""
    print("  Aggressive PostgreSQL cleanup...")

    if sys.platform == 'win32':
        try:
            subprocess.run('taskkill /F /IM postgres.exe /T', shell=True, capture_output=True, timeout=5)
            print("    Killed postgres.exe processes")
        except:
            pass
    else:
        subprocess.run(['pkill', '-9', '-f', 'postgres'], capture_output=True)

    time.sleep(1)

    for file_name in ['recovery.conf', 'recovery.signal', 'standby.signal', 'postmaster.pid']:
        file_path = Path(data_dir) / file_name
        if file_path.exists():
            file_path.unlink()
            print(f"    Removed {file_name}")


def stop_postgresql(data_dir, bin_path):
    """Stop PostgreSQL server properly."""
    global _postgresql_process

    if not bin_path:
        return

    pg_ctl_exe = Path(bin_path) / 'pg_ctl.exe' if sys.platform == 'win32' else Path(bin_path) / 'pg_ctl'

    if not pg_ctl_exe.exists():
        return

    print("  Stopping PostgreSQL...")

    try:
        subprocess.run(
            [str(pg_ctl_exe), '-D', data_dir, '-m', 'fast', 'stop'],
            capture_output=True,
            text=True,
            timeout=10
        )
    except:
        pass

    time.sleep(1)


def is_database_initialized(data_dir):
    """Check if PostgreSQL database is already initialized."""
    pg_version_file = Path(data_dir) / 'PG_VERSION'
    pg_hba_file = Path(data_dir) / 'pg_hba.conf'
    postgresql_conf = Path(data_dir) / 'postgresql.conf'
    return pg_version_file.exists() and pg_hba_file.exists() and postgresql_conf.exists()


def init_database(data_dir, bin_path):
    """Initialize PostgreSQL database in empty directory."""
    print("  Initializing new database...")
    initdb_exe = Path(bin_path) / 'initdb.exe' if sys.platform == 'win32' else Path(bin_path) / 'initdb'

    if not initdb_exe.exists():
        raise Exception(f"initdb not found at {initdb_exe}")

    cmd = [
        str(initdb_exe),
        '-D', data_dir,
        '--auth=trust',
        '--auth-local=trust',
        '--encoding=utf8',
        '-U', 'postgres'
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise Exception(f"initdb failed: {result.stderr}")

    print("  ✅ Database initialized")
    return True


def find_free_port(start_port=7412, max_port=65535):
    """Find a free port."""
    for port in range(start_port, max_port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(('127.0.0.1', port))
                return port
            except:
                continue
    raise Exception("No free ports available")


def wait_for_postgres(port, timeout=60):
    """Wait for PostgreSQL to accept connections."""
    print(f"    Waiting for PostgreSQL (timeout: {timeout}s)...")
    start = time.time()

    while time.time() - start < timeout:
        try:
            conn = psycopg2.connect(
                host='127.0.0.1',
                port=port,
                user='postgres',
                connect_timeout=2,
                sslmode='disable'
            )
            conn.close()
            elapsed = time.time() - start
            print(f"    PostgreSQL ready after {elapsed:.1f}s")
            return True
        except:
            time.sleep(0.5)

    raise Exception(f"PostgreSQL did not become ready within {timeout}s")


def cleanup_resources():
    """Clean up PostgreSQL server."""
    global _postgresql_server, _postgresql_bin_path, _postgresql_data_dir

    if _postgresql_data_dir and _postgresql_bin_path:
        stop_postgresql(_postgresql_data_dir, _postgresql_bin_path)

    _postgresql_server = None

    if hasattr(sys.stdout, 'log_file'):
        try:
            sys.stdout.log_file.close()
        except:
            pass


def start_postgresql(output_callback):
    """Start embedded PostgreSQL server with output to callback."""
    global _postgresql_server, _postgresql_bin_path, _postgresql_port, _postgresql_data_dir

    if sys.platform == 'win32':
        appdata = os.environ.get('APPDATA', os.path.expanduser('~'))
        data_dir = os.path.join(appdata, 'IGDB_Site_PostgreSQL')
    else:
        data_dir = os.path.join(os.path.expanduser('~'), '.igdb_site_postgresql')

    Path(data_dir).mkdir(parents=True, exist_ok=True)
    _postgresql_data_dir = data_dir

    output_callback(f"📁 Database: {data_dir}\n")

    try:
        output_callback("  Fixing timezonesets...\n")
        fix_timezonesets(data_dir)

        output_callback("  Checking for existing PostgreSQL processes...\n")
        kill_our_postgresql_processes(data_dir)
        cleanup_recovery_files(data_dir)

        output_callback("  Looking for PostgreSQL binaries...\n")
        bin_path = get_postgres_bin_path(output_callback)
        _postgresql_bin_path = bin_path
        output_callback(f"  PostgreSQL binaries: {bin_path}\n")

        output_callback("  Checking database initialization status...\n")
        if not is_database_initialized(data_dir):
            output_callback("  Database not initialized, cleaning directory...\n")
            if Path(data_dir).exists():
                for item in Path(data_dir).iterdir():
                    try:
                        if item.is_file():
                            item.unlink()
                        else:
                            shutil.rmtree(item)
                    except Exception as e:
                        output_callback(f"    Could not remove {item}: {e}\n")
            output_callback("  Initializing new database...\n")
            init_database(data_dir, bin_path)
        else:
            output_callback("  Database already initialized\n")

        output_callback("  Finding free port...\n")
        port = find_free_port(start_port=7412)
        _postgresql_port = port
        output_callback(f"  Using port: {port}\n")

        pg_ctl_exe = Path(bin_path) / 'pg_ctl.exe' if sys.platform == 'win32' else Path(bin_path) / 'pg_ctl'

        # Write port to postgresql.conf
        conf_file = Path(data_dir) / 'postgresql.conf'
        if conf_file.exists():
            with open(conf_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            with open(conf_file, 'w', encoding='utf-8') as f:
                for line in lines:
                    if not line.strip().startswith('port ='):
                        f.write(line)
            with open(conf_file, 'a', encoding='utf-8') as f:
                f.write(f"\nport = {port}\n")
        else:
            with open(conf_file, 'w', encoding='utf-8') as f:
                f.write(f"port = {port}\n")

        # Create logs directory for PostgreSQL
        if getattr(sys, 'frozen', False):
            exe_dir = Path(sys.executable).parent
        else:
            exe_dir = Path.cwd()

        pg_logs_dir = exe_dir / 'logs'
        pg_logs_dir.mkdir(exist_ok=True)
        pg_log_file = pg_logs_dir / f'postgresql_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

        output_callback(f"  Starting PostgreSQL on port {port}...\n")
        output_callback(f"  PostgreSQL log: {pg_log_file}\n")

        if sys.platform == 'win32':
            DETACHED_PROCESS = 0x00000008
            CREATE_NO_WINDOW = 0x08000000
            creationflags = DETACHED_PROCESS | CREATE_NO_WINDOW

            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags = subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0

            # Write PostgreSQL output to both log file and callback
            with open(pg_log_file, 'a', encoding='utf-8', errors='ignore') as log_f:
                process = subprocess.Popen(
                    [str(pg_ctl_exe), '-D', data_dir, 'start'],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.DEVNULL,
                    creationflags=creationflags,
                    startupinfo=startupinfo,
                    text=True,
                    encoding='utf-8',
                    errors='ignore'
                )

            def read_output(pipe, prefix):
                for line in iter(pipe.readline, ''):
                    if line.strip():
                        output_callback(f"{prefix}{line}")
                        with open(pg_log_file, 'a', encoding='utf-8', errors='ignore') as log_f:
                            log_f.write(line)

            threading.Thread(target=read_output, args=(process.stdout, ""), daemon=True).start()
            threading.Thread(target=read_output, args=(process.stderr, ""), daemon=True).start()
        else:
            with open(pg_log_file, 'a', encoding='utf-8', errors='ignore') as log_f:
                process = subprocess.Popen(
                    [str(pg_ctl_exe), '-D', data_dir, 'start'],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.DEVNULL,
                    text=True,
                    encoding='utf-8',
                    errors='ignore'
                )

            def read_output(pipe, prefix):
                for line in iter(pipe.readline, ''):
                    if line.strip():
                        output_callback(f"{prefix}{line}")
                        with open(pg_log_file, 'a', encoding='utf-8', errors='ignore') as log_f:
                            log_f.write(line)

            threading.Thread(target=read_output, args=(process.stdout, ""), daemon=True).start()
            threading.Thread(target=read_output, args=(process.stderr, ""), daemon=True).start()

        _postgresql_process = process

        output_callback(f"  Waiting for PostgreSQL to become ready (timeout: 60s)...\n")
        wait_for_postgres(port, timeout=60)

        database_url = f"postgres://postgres@127.0.0.1:{port}/postgres?sslmode=disable"
        os.environ['DATABASE_URL'] = database_url
        os.environ['PGSSLMODE'] = 'disable'

        _postgresql_server = True
        output_callback(f"✅ PostgreSQL ready on port {port}\n")
        output_callback(f"📁 PostgreSQL log: {pg_log_file}\n")
        return database_url

    except Exception as e:
        output_callback(f"❌ Failed to start PostgreSQL: {e}\n")
        traceback.print_exc()
        sys.exit(1)


# ============================================
# ФУНКЦИИ ДЛЯ РАБОТЫ С DJANGO
# ============================================

def setup_environment():
    """Configure Django environment."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'igdb_site.settings')
    os.environ['DESKTOP_MODE'] = '1'

    if getattr(sys, 'frozen', False):
        exe_dir = Path(sys.executable).parent
        sys.path.insert(0, str(exe_dir))
    else:
        current_dir = Path(__file__).parent
        sys.path.insert(0, str(current_dir))
        sys.path.insert(0, str(current_dir / 'igdb_site'))


def check_database_state():
    """Check if database tables and data exist."""
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
            print(f"📊 Games in database: {game_count}")

    return has_games_table, has_games_data


def import_keyword_category(data):
    """Import KeywordCategory data."""
    from games import models as game_models

    if 'KeywordCategory' not in data or not data['KeywordCategory']:
        return 0

    objects_data = data['KeywordCategory']
    print(f"  Importing KeywordCategory: {len(objects_data)}...")

    objects_to_create = []
    for item in objects_data:
        fields = item['fields'].copy()
        objects_to_create.append(game_models.KeywordCategory(id=item['pk'], **fields))

    batch_size = 5000
    saved_count = 0
    for i in range(0, len(objects_to_create), batch_size):
        batch = objects_to_create[i:i + batch_size]
        created = game_models.KeywordCategory.objects.bulk_create(batch, ignore_conflicts=True)
        saved_count += len(created)

    print(f"    ✅ Saved: {saved_count}")
    del objects_to_create
    gc.collect()
    return saved_count


def import_independent_model(data_key, model_class, data, results):
    """Import independent model data."""
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


def import_independent_models_parallel(data):
    """Import all independent models in parallel."""
    from games import models as game_models

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

    print("  Importing independent models...")
    results = {}

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for data_key, model_class in independent_models.items():
            future = executor.submit(import_independent_model, data_key, model_class, data, results)
            futures.append(future)

        for future in as_completed(futures):
            future.result()

    total = 0
    for data_key, count in results.items():
        if count:
            print(f"    ✅ {data_key}: {count}")
            total += count

    gc.collect()
    return total


def import_keywords(data):
    """Import Keyword data using raw SQL."""
    if 'Keyword' not in data or not data['Keyword']:
        return 0

    objects_data = data['Keyword']
    print(f"  Importing Keyword: {len(objects_data)}...")

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

    print(f"    ✅ Saved: {inserted}")
    del values_list
    gc.collect()
    return inserted


def prepare_game_data(games_data):
    """Prepare game data for bulk insert."""
    game_values = []
    game_parent_refs = []

    def sql_value(val, is_string=False):
        if val is None:
            return 'NULL'
        if is_string:
            if val == '':
                return "''"
            escaped = str(val).replace("'", "''")
            return f"'{escaped}'"
        return str(val)

    def array_value(val):
        if not val:
            return "'{}'"

        if isinstance(val, str):
            cleaned = val.strip('[]')
            if not cleaned:
                return "'{}'"
            return f"'{{{cleaned}}}'"

        if isinstance(val, list):
            if not val:
                return "'{}'"
            arr_str = ','.join(str(x) for x in val)
            return f"'{{{arr_str}}}'"

        return "'{}'"

    for game_data in games_data:
        game_id = game_data['pk']
        fields = game_data['fields'].copy()

        developers = fields.pop('developers', [])
        genres = fields.pop('genres', [])
        platforms = fields.pop('platforms', [])
        keywords = fields.pop('keywords', [])
        themes = fields.pop('themes', [])
        game_modes = fields.pop('game_modes', [])
        engines = fields.pop('engines', [])
        player_perspectives = fields.pop('player_perspectives', [])

        parent_game_id = fields.get('parent_game')
        version_parent_id = fields.get('version_parent')

        igdb_id = fields.get('igdb_id', 0)
        name = sql_value(fields.get('name', ''), True)
        summary = sql_value(fields.get('summary'), True)
        rating = sql_value(fields.get('rating'))
        rating_count = fields.get('rating_count', 0)
        first_release_date = sql_value(fields.get('first_release_date'), True)
        cover_url = sql_value(fields.get('cover_url'), True)

        date_added = sql_value(fields.get('date_added'), True)
        if date_added == 'NULL':
            date_added = 'NOW()'
        updated_at = sql_value(fields.get('updated_at'), True)
        if updated_at == 'NULL':
            updated_at = 'NOW()'

        developer_ids_value = fields.get('developer_ids', '[]')
        game_mode_ids_value = fields.get('game_mode_ids', '[]')
        genre_ids_value = fields.get('genre_ids', '[]')
        keyword_ids_value = fields.get('keyword_ids', '[]')
        theme_ids_value = fields.get('theme_ids', '[]')
        engine_ids_value = fields.get('engine_ids', '[]')
        perspective_ids_value = fields.get('perspective_ids', '[]')

        developer_ids = array_value(developer_ids_value)
        game_mode_ids = array_value(game_mode_ids_value)
        genre_ids = array_value(genre_ids_value)
        keyword_ids = array_value(keyword_ids_value)
        theme_ids = array_value(theme_ids_value)
        engine_ids = array_value(engine_ids_value)
        perspective_ids = array_value(perspective_ids_value)

        game_values.append(f"""({game_id}, {igdb_id}, {name}, {summary}, {rating}, {rating_count},
            {first_release_date}, {cover_url}, {developer_ids}, {game_mode_ids},
            {genre_ids}, {keyword_ids}, {theme_ids}, {perspective_ids}, {engine_ids}, {date_added}, {updated_at})""")

        game_parent_refs.append({
            'game_id': game_id,
            'parent_game_id': parent_game_id,
            'version_parent_id': version_parent_id,
        })

    return game_values, game_parent_refs


def insert_games(game_values, games_data):
    """Insert games using raw SQL."""
    if not game_values:
        return 0

    batch_size = 2000
    inserted = 0

    with connection.cursor() as cursor:
        for i in range(0, len(game_values), batch_size):
            batch = game_values[i:i + batch_size]
            sql = f"""
                INSERT INTO games_game (id, igdb_id, name, summary, rating, rating_count,
                    first_release_date, cover_url, developer_ids, game_mode_ids,
                    genre_ids, keyword_ids, theme_ids, perspective_ids, engine_ids, date_added, updated_at)
                VALUES {','.join(batch)}
                ON CONFLICT (id) DO NOTHING
            """
            cursor.execute(sql)
            inserted += cursor.rowcount

    print(f"    ✅ Games saved: {inserted}/{len(games_data)}")
    return inserted


def import_database_dump(output_callback):
    """Import full PostgreSQL dump with schema and data without dropping database."""

    if getattr(sys, 'frozen', False):
        exe_dir = Path(sys.executable).parent
    else:
        exe_dir = Path.cwd()

    dump_file = exe_dir / 'database.dump'
    output_callback(f"🔍 Looking for database dump at: {dump_file}\n")

    if not dump_file.exists():
        output_callback("ℹ️ No database dump found\n")
        return False

    dump_size_mb = dump_file.stat().st_size / (1024 * 1024)
    output_callback(f"📦 Found database dump: {dump_file.name} ({dump_size_mb:.1f} MB)\n")

    output_callback("🗜️ Restoring database schema and data...\n")

    bin_path = get_postgres_bin_path(output_callback)
    if not bin_path:
        output_callback("❌ PostgreSQL binaries not found\n")
        return False

    pg_restore_exe = Path(bin_path) / 'pg_restore.exe' if sys.platform == 'win32' else Path(bin_path) / 'pg_restore'

    if not pg_restore_exe.exists():
        output_callback(f"❌ pg_restore not found at {pg_restore_exe}\n")
        return False

    from django.conf import settings

    db_settings = settings.DATABASES['default']
    db_name = db_settings['NAME']
    db_user = db_settings['USER']
    db_host = db_settings.get('HOST', '127.0.0.1')
    db_port = db_settings.get('PORT', 5432)
    db_password = db_settings.get('PASSWORD', '')

    output_callback(f"  Target database: {db_name}\n")
    output_callback(f"  Host: {db_host}:{db_port}\n")

    tmp_dump_path = str(dump_file)

    try:
        env = os.environ.copy()
        if db_password:
            env['PGPASSWORD'] = db_password

        output_callback("  Restoring (this may take several minutes)...\n")

        import time
        start_time = time.time()

        restore_cmd = [
            str(pg_restore_exe),
            '--dbname', f'postgresql://{db_user}@{db_host}:{db_port}/{db_name}',
            '--no-owner',
            '--no-privileges',
            '--jobs', '4',
            '--verbose',
            tmp_dump_path
        ]

        result = subprocess.run(
            restore_cmd,
            env=env,
            capture_output=True,
            text=True
        )

        elapsed = time.time() - start_time

        if result.returncode != 0:
            stderr_output = result.stderr if result.stderr else ""
            if stderr_output:
                output_callback(f"  Restore had warnings: {stderr_output[:500]}\n")

        output_callback(f"  Restore completed in {elapsed:.1f} seconds\n")

        from django.db import connections
        connections.close_all()

        from django.db import connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM games_game;")
            game_count = cursor.fetchone()[0]
            output_callback(f"  ✅ Imported {game_count:,} games\n")

        from django.contrib.auth import get_user_model
        User = get_user_model()
        if User.objects.count() == 0:
            User.objects.create_superuser('admin', 'admin@localhost.com', 'admin')
            output_callback("👤 Created superuser: admin/admin\n")

        return True

    except Exception as e:
        output_callback(f"❌ Import failed: {e}\n")
        import traceback
        output_callback(traceback.format_exc())
        return False


def run_migrations_once(output_callback):
    """Import full database dump - no separate migrations needed."""
    output_callback("📦 Checking database state...\n")

    has_games_table, has_games_data = check_database_state()
    output_callback(f"📊 Games in database: {has_games_data}\n")

    if getattr(sys, 'frozen', False):
        exe_dir = Path(sys.executable).parent
    else:
        exe_dir = Path.cwd()

    import_marker = exe_dir / '.data_imported'

    if import_marker.exists() and has_games_data:
        output_callback("✅ Database has data, skipping import\n")
        return

    if not has_games_data:
        output_callback("📦 Database is empty, importing full database...\n")

        if import_database_dump(output_callback):
            import_marker.touch()
            output_callback("✅ Database import completed successfully\n")
        else:
            output_callback("⚠️ Could not import database dump\n")
            output_callback("   The application will run with empty database\n")


def run_django_server(output_callback):
    """Run Django server and redirect output."""
    if getattr(sys, 'frozen', False):
        exe_dir = Path(sys.executable).parent
    else:
        exe_dir = Path.cwd()

    creationflags = 0
    if sys.platform == 'win32':
        DETACHED_PROCESS = 0x00000008
        CREATE_NO_WINDOW = 0x08000000
        creationflags = DETACHED_PROCESS | CREATE_NO_WINDOW

    process = subprocess.Popen(
        [sys.executable, '-m', 'django', 'runserver', '--noreload', '127.0.0.1:8000'],
        cwd=exe_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=creationflags,
        text=True
    )

    def read_output(pipe):
        for line in iter(pipe.readline, ''):
            if line.strip():
                output_callback(line)

    threading.Thread(target=read_output, args=(process.stdout,), daemon=True).start()
    threading.Thread(target=read_output, args=(process.stderr,), daemon=True).start()

    return process


# ============================================
# GUI С ОДНИМ ТЕРМИНАЛОМ
# ============================================

def create_gui_window():
    """Create GUI window with single terminal where everything runs."""
    root = tk.Tk()
    root.title("gamespeek Desktop Launcher")
    root.geometry("1000x650")

    root.eval('tk::PlaceWindow . center')

    main_frame = ttk.Frame(root, padding="10")
    main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)
    main_frame.columnconfigure(0, weight=1)
    main_frame.rowconfigure(2, weight=1)

    title_label = tk.Label(main_frame, text="🎮 gamespeek",
                           font=("Arial", 16, "bold"))
    title_label.grid(row=0, column=0, pady=(0, 10), sticky=tk.W)

    control_frame = ttk.Frame(main_frame)
    control_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
    control_frame.columnconfigure(1, weight=1)

    status_label = tk.Label(control_frame, text="Starting...", font=("Arial", 10))
    status_label.grid(row=0, column=0, padx=(0, 10), sticky=tk.W)

    url_label = tk.Label(control_frame, text="http://127.0.0.1:8000",
                         font=("Arial", 10), fg="blue", cursor="hand2")
    url_label.grid(row=0, column=1, padx=(0, 10), sticky=tk.W)
    url_label.bind("<Button-1>", lambda e: webbrowser.open("http://127.0.0.1:8000"))
    url_label.config(state="disabled")

    progress_bar = ttk.Progressbar(control_frame, mode='indeterminate', length=150)
    progress_bar.grid(row=0, column=2, padx=(0, 10), sticky=tk.W)
    progress_bar.start()

    stop_button = tk.Button(control_frame, text="Stop",
                            command=lambda: shutdown_app(root),
                            bg="#ff4444", fg="white", padx=20, pady=5)
    stop_button.grid(row=0, column=3, sticky=tk.E)
    stop_button.config(state="disabled")

    separator = ttk.Separator(main_frame, orient='horizontal')
    separator.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=(0, 10))

    terminal_frame = ttk.LabelFrame(main_frame, text="Terminal Output", padding="5")
    terminal_frame.grid(row=3, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
    terminal_frame.columnconfigure(0, weight=1)
    terminal_frame.rowconfigure(1, weight=1)

    button_frame = ttk.Frame(terminal_frame)
    button_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 5))
    button_frame.columnconfigure(1, weight=1)

    copy_button = tk.Button(button_frame, text="📋 Copy Selected",
                            command=lambda: copy_selection(),
                            bg="#007acc", fg="white", padx=10, pady=2,
                            font=("Arial", 9))
    copy_button.grid(row=0, column=0, sticky=tk.W)

    select_all_button = tk.Button(button_frame, text="✓ Select All",
                                  command=lambda: select_all(),
                                  bg="#3c3c3c", fg="white", padx=10, pady=2,
                                  font=("Arial", 9))
    select_all_button.grid(row=0, column=1, sticky=tk.W, padx=(5, 0))

    clear_button = tk.Button(button_frame, text="🗑️ Clear",
                             command=lambda: clear_terminal(),
                             bg="#3c3c3c", fg="white", padx=10, pady=2,
                             font=("Arial", 9))
    clear_button.grid(row=0, column=2, sticky=tk.W, padx=(5, 0))

    terminal_text = tk.Text(terminal_frame, wrap=tk.WORD, font=("Consolas", 9),
                            bg="#1e1e1e", fg="#d4d4d4", insertbackground="white",
                            relief=tk.FLAT, borderwidth=0,
                            selectbackground="#264f78", selectforeground="white")
    terminal_text.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

    scrollbar = ttk.Scrollbar(terminal_frame, orient=tk.VERTICAL, command=terminal_text.yview)
    scrollbar.grid(row=1, column=1, sticky=(tk.N, tk.S))
    terminal_text.config(yscrollcommand=scrollbar.set)

    terminal_text.tag_configure("INFO", foreground="#4ec9b0")
    terminal_text.tag_configure("ERROR", foreground="#f48771")
    terminal_text.tag_configure("WARNING", foreground="#ce9178")
    terminal_text.tag_configure("SUCCESS", foreground="#6a9955")
    terminal_text.tag_configure("DEBUG", foreground="#9cdcfe")
    terminal_text.tag_configure("TIME", foreground="#d7ba7d")

    def copy_selection():
        try:
            selected = terminal_text.get(tk.SEL_FIRST, tk.SEL_LAST)
            root.clipboard_clear()
            root.clipboard_append(selected)
            root.update()
            copy_button.config(bg="#00aa00")
            root.after(200, lambda: copy_button.config(bg="#007acc"))
        except tk.TclError:
            copy_button.config(bg="#aa0000")
            root.after(200, lambda: copy_button.config(bg="#007acc"))

    def select_all():
        terminal_text.tag_add(tk.SEL, "1.0", tk.END)
        terminal_text.mark_set(tk.INSERT, "1.0")
        terminal_text.see(tk.INSERT)
        select_all_button.config(bg="#00aa00")
        root.after(200, lambda: select_all_button.config(bg="#3c3c3c"))

    def clear_terminal():
        terminal_text.delete("1.0", tk.END)
        clear_button.config(bg="#00aa00")
        root.after(200, lambda: clear_button.config(bg="#3c3c3c"))

    info_label = tk.Label(main_frame,
                          text="💡 Close window to shutdown | Select text and click 'Copy Selected' button",
                          font=("Arial", 9), fg="gray")
    info_label.grid(row=4, column=0, sticky=(tk.W, tk.E), pady=(0, 5))

    is_shutting_down = False

    def append_to_terminal(message, tag="INFO"):
        def _append():
            terminal_text.insert(tk.END, message, tag)
            terminal_text.see(tk.END)
            terminal_text.yview_moveto(1.0)

        root.after(0, _append)

    def update_status(text, enable_url=False, enable_stop=False, stop_progress=False):
        def _update():
            status_label.config(text=text)
            if enable_url:
                url_label.config(state="normal")
            if enable_stop:
                stop_button.config(state="normal")
            if stop_progress:
                progress_bar.stop()
                progress_bar.grid_remove()

        root.after(0, _update)

    def shutdown_app(window=None):
        nonlocal is_shutting_down
        if is_shutting_down:
            return
        is_shutting_down = True

        append_to_terminal("\n" + "=" * 50 + "\n", "INFO")
        append_to_terminal("🛑 Shutting down...\n", "WARNING")
        append_to_terminal("=" * 50 + "\n", "INFO")

        global server_process
        if server_process and server_process.poll() is None:
            append_to_terminal("Stopping Django...\n", "INFO")
            server_process.terminate()
            try:
                server_process.wait(timeout=5)
            except:
                server_process.kill()

        append_to_terminal("Stopping PostgreSQL...\n", "INFO")
        cleanup_resources()
        append_to_terminal("✅ Shutdown complete\n", "SUCCESS")

        if window:
            window.destroy()
        else:
            root.destroy()

        os._exit(0)

    def on_closing():
        shutdown_app(root)

    root.protocol("WM_DELETE_WINDOW", on_closing)

    startup_queue = queue.Queue()

    def run_startup():
        try:
            append_to_terminal("Starting PostgreSQL...\n")
            setup_environment()
            database_url = start_postgresql(append_to_terminal)
            os.environ['DATABASE_URL'] = database_url

            append_to_terminal("\nLoading Django...\n")
            django.setup()

            if getattr(sys, 'frozen', False):
                exe_dir = Path(sys.executable).parent
                static_root = exe_dir / 'staticfiles'
                if static_root.exists():
                    settings.STATIC_ROOT = str(static_root)
                    settings.STATIC_URL = '/static/'

            run_migrations_once(append_to_terminal)

            append_to_terminal("\nStarting web server...\n")
            global server_process
            server_process = run_django_server(append_to_terminal)

            append_to_terminal("\n✅ SERVER READY! http://127.0.0.1:8000\n", "SUCCESS")
            startup_queue.put("SERVER_READY")

        except Exception as e:
            append_to_terminal(f"\n❌ Fatal error: {e}\n", "ERROR")
            traceback.print_exc(file=sys.stderr)
            startup_queue.put("STARTUP_FAILED")

    threading.Thread(target=run_startup, daemon=True).start()

    def check_queue():
        try:
            msg = startup_queue.get_nowait()
            if msg == "SERVER_READY":
                update_status("Running", enable_url=True, enable_stop=True, stop_progress=True)
            elif msg == "STARTUP_FAILED":
                update_status("Failed!", enable_stop=True, stop_progress=True)
        except queue.Empty:
            pass
        finally:
            root.after(100, check_queue)

    root.after(100, check_queue)
    root.mainloop()
    return root


# ============================================
# MAIN
# ============================================

def main():
    """Main entry point."""
    if getattr(sys, 'frozen', False):
        exe_dir = Path(sys.executable).parent
    else:
        exe_dir = Path.cwd()

    logs_dir = exe_dir / 'logs'
    logs_dir.mkdir(exist_ok=True)

    log_filename = logs_dir / f'gamespeek_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_file = open(log_filename, 'w', encoding='utf-8', buffering=1)

    stdout_logger = TeeLogger(log_file, sys.__stdout__, is_error=False, terminal_callback=None)
    stderr_logger = TeeLogger(log_file, sys.__stderr__, is_error=True, terminal_callback=None)

    original_stdout = sys.stdout
    original_stderr = sys.stderr

    sys.stdout = stdout_logger
    sys.stderr = stderr_logger
    sys.stdout.log_file = log_file
    sys.stderr.log_file = log_file

    global server_process
    server_process = None

    try:
        create_gui_window()
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
    finally:
        if server_process and server_process.poll() is None:
            server_process.terminate()
            server_process.wait(timeout=5)

        cleanup_resources()
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        log_file.close()
        print(f"Log: {log_filename}")


if __name__ == '__main__':
    main()