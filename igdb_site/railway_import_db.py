#!/usr/bin/env python
"""
Railway Database Import Script

This script imports database.dump into Railway PostgreSQL database.
Run this script ONCE after deploying to Railway to populate your database.

Usage on Railway:
    python railway_import_db.py

The script will:
    1. Read DATABASE_URL from environment (set by Railway)
    2. Import the dump using pg_restore (same logic as run.py)
"""

import os
import sys
import subprocess
from pathlib import Path
import django


# ============================================
# SETUP DJANGO ENVIRONMENT
# ============================================

def setup_django():
    """Configure Django environment for Railway."""
    # Add the project directory to Python path
    current_dir = Path(__file__).parent
    sys.path.insert(0, str(current_dir))
    sys.path.insert(0, str(current_dir / 'igdb_site'))

    # Set Django settings module
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'igdb_site.settings')

    # Initialize Django
    django.setup()


# ============================================
# DATABASE IMPORT FUNCTIONS (from run.py)
# ============================================

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
            print(f"📊 Games in database: {game_count}")

    return has_games_table, has_games_data


def find_pg_restore():
    """Find pg_restore executable."""
    # Check common locations for pg_restore
    possible_paths = [
        '/usr/bin/pg_restore',
        '/usr/local/bin/pg_restore',
        '/usr/lib/postgresql/*/bin/pg_restore',
    ]

    for path_pattern in possible_paths:
        from glob import glob
        for path in glob(path_pattern):
            if Path(path).exists():
                return path

    # Try to find via which command
    try:
        result = subprocess.run(['which', 'pg_restore'], capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
    except:
        pass

    return None


def import_database_dump(output_callback=None):
    """
    Import full PostgreSQL dump with schema and data without dropping database.
    Same logic as in run.py but adapted for Railway.
    """
    if output_callback is None:
        output_callback = print

    # Find the database dump file
    if getattr(sys, 'frozen', False):
        exe_dir = Path(sys.executable).parent
    else:
        exe_dir = Path.cwd()

    # Check multiple possible locations for the dump file
    dump_locations = [
        exe_dir / 'database.dump',
        exe_dir / 'igdb_site' / 'database.dump',
        Path('/app') / 'database.dump',
        Path('/app') / 'igdb_site' / 'database.dump',
    ]

    dump_file = None
    for location in dump_locations:
        if location.exists():
            dump_file = location
            break

    if not dump_file:
        output_callback("ℹ️ No database dump found\n")
        return False

    dump_size_mb = dump_file.stat().st_size / (1024 * 1024)
    output_callback(f"📦 Found database dump: {dump_file.name} ({dump_size_mb:.1f} MB)\n")

    output_callback("🗜️ Restoring database schema and data...\n")

    # Find pg_restore
    pg_restore_path = find_pg_restore()
    if not pg_restore_path:
        output_callback("❌ pg_restore not found. Please install postgresql-client package.\n")
        output_callback("   Run: apt-get update && apt-get install -y postgresql-client\n")
        return False

    output_callback(f"  Using pg_restore: {pg_restore_path}\n")

    # Get database settings from Django
    from django.conf import settings

    db_settings = settings.DATABASES['default']
    db_name = db_settings['NAME']
    db_user = db_settings['USER']
    db_host = db_settings.get('HOST', '127.0.0.1')
    db_port = db_settings.get('PORT', 5432)
    db_password = db_settings.get('PASSWORD', '')

    output_callback(f"  Target database: {db_name}\n")
    output_callback(f"  Host: {db_host}:{db_port}\n")

    try:
        env = os.environ.copy()
        if db_password:
            env['PGPASSWORD'] = db_password

        output_callback("  Restoring (this may take several minutes)...\n")

        import time
        start_time = time.time()

        # Build connection string
        conn_string = f"postgresql://{db_user}@{db_host}:{db_port}/{db_name}"

        restore_cmd = [
            pg_restore_path,
            '--dbname', conn_string,
            '--no-owner',
            '--no-privileges',
            '--jobs', '4',
            '--verbose',
            str(dump_file)
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

        # Close all database connections
        from django.db import connections
        connections.close_all()

        # Verify import
        from django.db import connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM games_game;")
            game_count = cursor.fetchone()[0]
            output_callback(f"  ✅ Imported {game_count:,} games\n")

        # Create superuser if none exists
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


# ============================================
# MAIN
# ============================================

def main():
    """Main entry point for Railway import script."""
    print("=" * 50)
    print("Railway Database Import Tool")
    print("=" * 50)

    # Check if DATABASE_URL exists
    if not os.environ.get('DATABASE_URL'):
        print("❌ ERROR: DATABASE_URL not found in environment")
        print("   Make sure you have added a PostgreSQL database to your Railway service")
        print("   and linked it to your Django service via Variables")
        sys.exit(1)

    print(f"✅ DATABASE_URL found")

    # Setup Django
    print("📦 Setting up Django...")
    setup_django()

    # Check current database state
    print("📊 Checking database state...")
    has_games_table, has_games_data = check_database_state()

    if has_games_data:
        print("✅ Database already has data, skipping import")
        return

    if not has_games_table:
        print("📦 Database has no tables, running migrations first...")
        from django.core.management import call_command
        call_command('migrate', verbosity=1)

    # Import the database dump
    print("📦 Importing database dump...")
    if import_database_dump(print):
        # Create marker file to skip import on next runs
        marker_file = Path('/app/.data_imported')
        marker_file.touch()
        print("✅ Database import completed successfully")
    else:
        print("⚠️ Could not import database dump")
        print("   The application will run with empty database")


if __name__ == '__main__':
    main()