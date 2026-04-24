#!/usr/bin/env python

"""
Export/Import Django database to/from PostgreSQL native dump with schema and data.
Usage:
    python export_db.py export
    python export_db.py import
"""

import os
import sys
import subprocess
import django
import shutil
import time
import argparse
from pathlib import Path
from django.conf import settings


def setup_django():
    """Setup Django environment."""
    PROJECT_ROOT = Path(__file__).parent
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'igdb_site.settings')
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    django.setup()


def find_postgres_bin(executable_name):
    """
    Find PostgreSQL executable (pg_dump or pg_restore) with priority for version 18.

    Args:
        executable_name: Name of executable ('pg_dump' or 'pg_restore')

    Returns:
        Path to executable
    """
    # Check if executable is in PATH first (works on Linux VPS)
    which_cmd = shutil.which(executable_name)
    if which_cmd:
        print(f"  Found in PATH: {which_cmd}")
        return which_cmd

    # Check environment variable for custom PostgreSQL path
    pg_path = os.getenv('POSTGRESQL_PATH')
    if pg_path:
        potential_path = Path(pg_path) / 'bin' / executable_name
        if potential_path.exists():
            print(f"  Using POSTGRESQL_PATH: {potential_path}")
            return str(potential_path)
        potential_path_exe = Path(pg_path) / 'bin' / f'{executable_name}.exe'
        if potential_path_exe.exists():
            print(f"  Using POSTGRESQL_PATH: {potential_path_exe}")
            return str(potential_path_exe)

    # Check bundled PostgreSQL (for compiled Windows app)
    bundled_paths = [
        Path(__file__).parent / 'PostgreSQL' / '18' / 'bin' / executable_name,
        Path(__file__).parent / 'PostgreSQL' / '18' / 'bin' / f'{executable_name}.exe',
        Path(__file__).parent / 'PostgreSQL' / 'bin' / executable_name,
        Path(__file__).parent / 'PostgreSQL' / 'bin' / f'{executable_name}.exe',
        Path(__file__).parent / 'dist' / 'gamespeek' / 'PostgreSQL' / '18' / 'bin' / executable_name,
        Path(__file__).parent / 'dist' / 'gamespeek' / 'PostgreSQL' / '18' / 'bin' / f'{executable_name}.exe',
    ]

    for path in bundled_paths:
        if path and path.exists():
            print(f"  Using bundled PostgreSQL: {path}")
            return str(path)

    # Check common Windows installation paths for PostgreSQL 18
    windows_paths = [
        f'C:\\Program Files\\PostgreSQL\\18\\bin\\{executable_name}.exe',
        f'C:\\Program Files\\PostgreSQL\\18\\bin\\{executable_name}',
        f'C:\\Program Files (x86)\\PostgreSQL\\18\\bin\\{executable_name}.exe',
        f'C:\\Program Files (x86)\\PostgreSQL\\18\\bin\\{executable_name}',
    ]

    for path in windows_paths:
        if Path(path).exists():
            print(f"  Using system PostgreSQL: {path}")
            return path

    # Check Linux paths (for VPS)
    linux_paths = [
        '/usr/bin/pg_dump',
        '/usr/local/bin/pg_dump',
        '/usr/pgsql-18/bin/pg_dump',
        '/usr/lib/postgresql/18/bin/pg_dump',
    ]

    for path in linux_paths:
        if Path(path).exists():
            print(f"  Using system PostgreSQL: {path}")
            return path

    raise Exception(
        f"{executable_name} not found!\n"
        f"Please ensure PostgreSQL 18 is installed or:\n"
        f"1. Add PostgreSQL bin directory to PATH, or\n"
        f"2. Set POSTGRESQL_PATH environment variable to PostgreSQL installation directory"
    )


def get_database_connection_params():
    """Extract database connection parameters from Django settings."""
    db_settings = settings.DATABASES['default']

    return {
        'db_name': db_settings['NAME'],
        'db_user': db_settings['USER'],
        'db_host': db_settings.get('HOST', 'localhost'),
        'db_port': db_settings.get('PORT', '5432'),
        'db_password': db_settings.get('PASSWORD', ''),
    }


def export_database(dump_file_path):
    """
    Export database to full PostgreSQL native dump.

    Args:
        dump_file_path: Path where to save the dump file

    Returns:
        bool: True if export successful, False otherwise
    """
    print("=" * 60)
    print("📤 DATABASE EXPORTER (Full dump with schema)")
    print("=" * 60)
    print()

    setup_django()

    db_params = get_database_connection_params()

    print(f"📁 Database: {db_params['db_name']}")
    print(f"👤 User: {db_params['db_user']}")
    print(f"🔗 Host: {db_params['db_host']}:{db_params['db_port']}")
    print()
    print(f"📁 Output: {dump_file_path.name}")
    print()

    try:
        pg_dump_path = find_postgres_bin('pg_dump')
        print(f"🔧 Using pg_dump: {pg_dump_path}")
        print()
    except Exception as e:
        print(f"❌ {e}")
        return False

    # Build connection string
    conn_string = f'postgresql://{db_params["db_user"]}@{db_params["db_host"]}:{db_params["db_port"]}/{db_params["db_name"]}'

    cmd = [
        pg_dump_path,
        '--dbname', conn_string,
        '--format', 'custom',
        '--compress', '9',
        '--no-sync',
        '--verbose',
    ]

    env = os.environ.copy()
    if db_params['db_password']:
        env['PGPASSWORD'] = db_params['db_password']

    print("⏳ Exporting full database with schema and data...")
    print("   (This may take several minutes)")
    print()

    start_time = time.time()

    with open(dump_file_path, 'wb') as f:
        result = subprocess.run(
            cmd,
            env=env,
            stdout=f,
            stderr=subprocess.PIPE,
            text=True
        )

    elapsed = time.time() - start_time

    if result.returncode != 0:
        print("❌ Export failed!")
        if result.stderr:
            print(f"Error: {result.stderr[:500]}")
        return False

    dump_size_mb = dump_file_path.stat().st_size / (1024 * 1024)
    print(f"✅ Full dump created: {dump_file_path.name}")
    print(f"   Size: {dump_size_mb:.1f} MB")
    print(f"   Time: {elapsed:.1f} seconds")

    print()
    print("=" * 60)
    print("✅ EXPORT COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print(f"\n📊 Summary:")
    print(f"   Total time: {elapsed:.1f} seconds")
    print(f"   Final size: {dump_size_mb:.1f} MB")
    print()

    return True


def import_database(dump_file_path):
    """
    Import database from PostgreSQL native dump with proper cleanup.

    Args:
        dump_file_path: Path to the dump file to import

    Returns:
        bool: True if import successful, False otherwise
    """
    print("=" * 60)
    print("📥 DATABASE IMPORTER (Restore from dump)")
    print("=" * 60)
    print()

    setup_django()

    if not dump_file_path.exists():
        print(f"❌ Dump file not found: {dump_file_path}")
        return False

    db_params = get_database_connection_params()

    print(f"📁 Source dump: {dump_file_path.name}")
    dump_size_mb = dump_file_path.stat().st_size / (1024 * 1024)
    print(f"   Size: {dump_size_mb:.1f} MB")
    print()
    print(f"🎯 Target database: {db_params['db_name']}")
    print(f"👤 User: {db_params['db_user']}")
    print(f"🔗 Host: {db_params['db_host']}:{db_params['db_port']}")
    print()

    try:
        pg_restore_path = find_postgres_bin('pg_restore')
        print(f"🔧 Using pg_restore: {pg_restore_path}")
        print()
    except Exception as e:
        print(f"❌ {e}")
        return False

    # Confirm before overwriting database
    print("⚠️  WARNING: This will DROP and RECREATE the entire database!")
    print("   All existing data will be lost.")
    print()
    response = input("   Do you want to continue? (yes/no): ")

    if response.lower() != 'yes':
        print("❌ Import cancelled by user.")
        return False

    print()
    print("⏳ Dropping and recreating database...")

    # Create connection without database name to perform admin operations
    admin_conn_params = {
        'db_name': 'postgres',
        'db_user': db_params['db_user'],
        'db_host': db_params['db_host'],
        'db_port': db_params['db_port'],
        'db_password': db_params['db_password'],
    }

    env = os.environ.copy()
    if admin_conn_params['db_password']:
        env['PGPASSWORD'] = admin_conn_params['db_password']

    # Find psql executable for admin operations
    try:
        psql_path = find_postgres_bin('psql')
    except Exception:
        # Try to use pg_restore path as base for psql
        psql_path = str(Path(pg_restore_path).parent / 'psql')
        if not Path(psql_path).exists():
            psql_path = 'psql'  # Hope it's in PATH

    # Terminate all connections to the target database
    terminate_cmd = [
        psql_path,
        '-h', admin_conn_params['db_host'],
        '-p', admin_conn_params['db_port'],
        '-U', admin_conn_params['db_user'],
        '-d', admin_conn_params['db_name'],
        '-c', f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db_params['db_name']}';"
    ]

    terminate_result = subprocess.run(
        terminate_cmd,
        env=env,
        capture_output=True,
        text=True
    )

    # Drop database if exists
    drop_cmd = [
        psql_path,
        '-h', admin_conn_params['db_host'],
        '-p', admin_conn_params['db_port'],
        '-U', admin_conn_params['db_user'],
        '-d', admin_conn_params['db_name'],
        '-c', f"DROP DATABASE IF EXISTS {db_params['db_name']};"
    ]

    drop_result = subprocess.run(
        drop_cmd,
        env=env,
        capture_output=True,
        text=True
    )

    if drop_result.returncode != 0:
        print(f"❌ Failed to drop database: {drop_result.stderr}")
        return False

    # Create new database
    create_cmd = [
        psql_path,
        '-h', admin_conn_params['db_host'],
        '-p', admin_conn_params['db_port'],
        '-U', admin_conn_params['db_user'],
        '-d', admin_conn_params['db_name'],
        '-c', f"CREATE DATABASE {db_params['db_name']} ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0;"
    ]

    create_result = subprocess.run(
        create_cmd,
        env=env,
        capture_output=True,
        text=True
    )

    if create_result.returncode != 0:
        print(f"❌ Failed to create database: {create_result.stderr}")
        return False

    print("✅ Database recreated")
    print()
    print("⏳ Restoring data from dump...")
    print("   (This may take several minutes)")
    print()

    # Restore the dump with optimization flags
    restore_cmd = [
        pg_restore_path,
        '--dbname',
        f'postgresql://{db_params["db_user"]}@{db_params["db_host"]}:{db_params["db_port"]}/{db_params["db_name"]}',
        '--verbose',
        '--no-owner',
    ]

    # Add parallel jobs only on Linux (Windows sometimes has issues)
    if sys.platform != 'win32':
        restore_cmd.extend(['--jobs', '4'])

    start_time = time.time()

    with open(dump_file_path, 'rb') as f:
        result = subprocess.run(
            restore_cmd,
            env=env,
            stdin=f,
            stderr=subprocess.PIPE,
            text=True
        )

    elapsed = time.time() - start_time

    if result.returncode != 0:
        print("❌ Import failed!")
        if result.stderr:
            print(f"Error: {result.stderr[:500]}")
        return False

    print(f"✅ Database restored successfully!")
    print(f"   Time: {elapsed:.1f} seconds")

    print()
    print("=" * 60)
    print("✅ IMPORT COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print(f"\n📊 Summary:")
    print(f"   Restore time: {elapsed:.1f} seconds")
    print(f"   Source size: {dump_size_mb:.1f} MB")
    print()
    print("Next steps:")
    print("1. Restart your Django application")
    print("2. Verify data integrity in admin panel")
    print()

    return True


def main():
    """Main entry point with command line argument parsing."""
    parser = argparse.ArgumentParser(
        description='Export or import Django database to/from PostgreSQL native dump',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python export_db.py export          # Export database to database.dump
    python export_db.py import          # Import database from database.dump
    python export_db.py import my.dump  # Import specific dump file

Environment variables:
    POSTGRESQL_PATH - Path to PostgreSQL installation directory
        """
    )

    parser.add_argument(
        'command',
        choices=['export', 'import'],
        help='Command to execute: export or import'
    )

    parser.add_argument(
        'dump_file',
        nargs='?',
        default='database.dump',
        help='Dump file name (default: database.dump)'
    )

    args = parser.parse_args()

    project_root = Path(__file__).parent
    dump_file_path = project_root / args.dump_file

    try:
        if args.command == 'export':
            success = export_database(dump_file_path)
        elif args.command == 'import':
            success = import_database(dump_file_path)
        else:
            print(f"❌ Unknown command: {args.command}")
            success = False

        sys.exit(0 if success else 1)

    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()