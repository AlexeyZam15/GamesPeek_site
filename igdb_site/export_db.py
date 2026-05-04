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


def export_database(dump_file_path, upload_to_vps_flag=False):
    """
    Export database to full PostgreSQL native dump.

    Args:
        dump_file_path: Path where to save the dump file
        upload_to_vps_flag: If True, upload the dump to VPS server after export

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

    # Upload to VPS if flag is set
    if upload_to_vps_flag:
        print("\n" + "=" * 60)
        print("📤 UPLOADING TO VPS SERVER")
        print("=" * 60)
        print()

        ssh_password = os.getenv('SSH_PASSWORD')
        vps_host = os.getenv('VPS_HOST')
        vps_user = os.getenv('VPS_USER')
        vps_path = os.getenv('VPS_PATH')

        # Validate required environment variables
        missing_vars = []
        if not ssh_password:
            missing_vars.append('SSH_PASSWORD')
        if not vps_host:
            missing_vars.append('VPS_HOST')
        if not vps_user:
            missing_vars.append('VPS_USER')
        if not vps_path:
            missing_vars.append('VPS_PATH')

        if missing_vars:
            print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
            print("   Please set them in your .env file:")
            print("   SSH_PASSWORD=your_password")
            print("   VPS_HOST=138.124.18.244")
            print("   VPS_USER=root")
            print("   VPS_PATH=/home/django/igdb_site/igdb_site/")
            print("   Upload skipped")
        else:
            upload_success = upload_to_vps(
                dump_file_path,
                ssh_password,
                vps_host,
                vps_user,
                vps_path
            )

            if upload_success:
                print("\n✅ Full workflow completed: Export + Upload to VPS")
            else:
                print("\n⚠️  Export completed but upload to VPS failed")
                print("   You can manually upload the file later")

    return True


def upload_to_vps(dump_file_path, ssh_password, vps_host, vps_user, vps_path):
    """
    Upload dump file to VPS server via SCP.

    Args:
        dump_file_path: Path to the dump file to upload
        ssh_password: SSH password for authentication
        vps_host: VPS server hostname or IP address
        vps_user: SSH username for VPS
        vps_path: Remote directory path on VPS

    Returns:
        bool: True if upload successful, False otherwise
    """
    # Ensure remote path ends with slash
    if not vps_path.endswith('/'):
        vps_path += '/'

    # Create full remote path with filename
    remote_full_path = f"{vps_user}@{vps_host}:{vps_path}{dump_file_path.name}"

    print(f"📡 Target: {vps_user}@{vps_host}")
    print(f"📁 Remote path: {vps_path}")
    print(f"📄 File: {dump_file_path.name}")
    print()

    # Use sshpass to provide password to scp
    sshpass_path = shutil.which('sshpass')

    if not sshpass_path:
        print("❌ 'sshpass' tool not found. Please install it:")
        print("   - Windows: choco install sshpass or download from https://sourceforge.net/projects/sshpass/")
        print("   - Linux: sudo apt-get install sshpass")
        print("   - macOS: brew install hudochenkov/sshpass/sshpass")
        return False

    # Build scp command with sshpass
    cmd = [
        sshpass_path,
        '-p', ssh_password,
        'scp',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        str(dump_file_path),
        remote_full_path
    ]

    print("⏳ Uploading to VPS server...")
    print("   (This may take several minutes depending on file size)")
    print()

    start_upload = time.time()

    try:
        result = subprocess.run(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            timeout=3600
        )

        upload_elapsed = time.time() - start_upload

        if result.returncode != 0:
            print(f"❌ Upload failed with exit code {result.returncode}")
            if result.stderr:
                if "Warning: Permanently added" not in result.stderr:
                    print(f"   Error: {result.stderr[:500]}")
            if result.stdout:
                print(f"   Output: {result.stdout[:500]}")
            return False

        print(f"✅ Upload completed successfully!")
        print(f"   Time: {upload_elapsed:.1f} seconds")
        print(f"   Remote location: {vps_path}{dump_file_path.name}")
        return True

    except subprocess.TimeoutExpired:
        print("❌ Upload timeout after 1 hour")
        return False
    except Exception as e:
        print(f"❌ Upload error: {e}")
        return False


def main():
    """Main entry point with command line argument parsing."""
    parser = argparse.ArgumentParser(
        description='Export or import Django database to/from PostgreSQL native dump',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python export_db.py              # Export database to database.dump (default)
    python export_db.py export       # Export database to database.dump
    python export_db.py export my.dump  # Export to specific dump file
    python export_db.py export --upload  # Export and upload to VPS
    python export_db.py export my.dump --upload  # Export to specific file and upload to VPS
    python export_db.py import       # Import database from database.dump
    python export_db.py import my.dump  # Import from specific dump file

Environment variables:
    POSTGRESQL_PATH - Path to PostgreSQL installation directory
    SSH_PASSWORD - Password for VPS server (required for --upload option)
    VPS_HOST - VPS server hostname or IP address (default: 138.124.18.244)
    VPS_USER - SSH username for VPS (default: root)
    VPS_PATH - Remote directory path on VPS (default: /home/django/igdb_site/igdb_site/)
        """
    )

    parser.add_argument(
        'command',
        nargs='?',
        default='export',
        choices=['export', 'import'],
        help='Command to execute: export or import (default: export)'
    )

    parser.add_argument(
        'dump_file',
        nargs='?',
        default='database.dump',
        help='Dump file name (default: database.dump)'
    )

    parser.add_argument(
        '--upload',
        action='store_true',
        help='Upload the dump file to VPS server after export (requires SSH_PASSWORD, VPS_HOST, VPS_USER, VPS_PATH in .env)'
    )

    args = parser.parse_args()

    project_root = Path(__file__).parent
    dump_file_path = project_root / args.dump_file

    try:
        if args.command == 'export':
            success = export_database(dump_file_path, args.upload)
            sys.exit(0 if success else 1)

        elif args.command == 'import':
            setup_django()

            if not dump_file_path.exists():
                print(f"❌ Dump file not found: {dump_file_path}")
                sys.exit(1)

            db_params = get_database_connection_params()

            dump_size_mb = dump_file_path.stat().st_size / (1024 * 1024)
            print(f"📁 Source dump: {dump_file_path.name}")
            print(f"   Size: {dump_size_mb:.1f} MB")
            print()
            print(f"🎯 Target database: {db_params['db_name']}")
            print(f"👤 User: {db_params['db_user']}")
            print(f"🔗 Host: {db_params['db_host']}:{db_params['db_port']}")
            print()

            try:
                pg_restore_path = find_postgres_bin('pg_restore')
                psql_path = find_postgres_bin('psql')
            except Exception as e:
                print(f"❌ {e}")
                sys.exit(1)

            # Подтверждение перед импортом
            print("⚠️  WARNING: This will DROP and RECREATE the entire database!")
            print("   All existing data will be lost.")
            print()
            response = input("   Do you want to continue? (yes/no): ")

            if response.lower() != 'yes':
                print("❌ Import cancelled by user.")
                sys.exit(0)

            print()
            print("⏳ Dropping and recreating database...")
            print()

            # Настройки для административных операций (подключаемся к БД postgres)
            admin_db_name = 'postgres'

            # Формируем строку подключения к админской БД
            admin_conn_string = f'postgresql://{db_params["db_user"]}@{db_params["db_host"]}:{db_params["db_port"]}/{admin_db_name}'

            env = os.environ.copy()
            if db_params['db_password']:
                env['PGPASSWORD'] = db_params['db_password']

            # 1. Завершаем все подключения к целевой БД
            print("   Terminating existing connections...")
            terminate_sql = f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db_params['db_name']}';"
            subprocess.run(
                [psql_path, admin_conn_string, '-c', terminate_sql],
                env=env,
                capture_output=True,
                text=True
            )

            # 2. Удаляем базу данных если существует
            print("   Dropping database...")
            drop_result = subprocess.run(
                [psql_path, admin_conn_string, '-c', f'DROP DATABASE IF EXISTS {db_params["db_name"]};'],
                env=env,
                capture_output=True,
                text=True
            )

            if drop_result.returncode != 0:
                print(f"❌ Failed to drop database: {drop_result.stderr}")
                sys.exit(1)

            # 3. Создаем новую базу данных
            print("   Creating new database...")
            create_result = subprocess.run(
                [psql_path, admin_conn_string, '-c',
                 f'CREATE DATABASE {db_params["db_name"]} ENCODING "UTF8" LC_COLLATE "C" LC_CTYPE "C" TEMPLATE template0;'],
                env=env,
                capture_output=True,
                text=True
            )

            if create_result.returncode != 0:
                print(f"❌ Failed to create database: {create_result.stderr}")
                sys.exit(1)

            print("✅ Database recreated")
            print()
            print("⏳ Restoring data from dump...")
            print()

            # Формируем строку подключения к новой БД
            conn_string = f'postgresql://{db_params["db_user"]}@{db_params["db_host"]}:{db_params["db_port"]}/{db_params["db_name"]}'

            # Команда pg_restore
            restore_cmd = [
                pg_restore_path,
                '--dbname', conn_string,
                '--verbose',
                '--no-owner',
                '--jobs', '4'
            ]

            # Добавляем файл дампа
            restore_cmd.append(str(dump_file_path))

            start_time = time.time()

            # Выполняем восстановление
            result = subprocess.run(
                restore_cmd,
                env=env,
                stderr=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True
            )

            elapsed = time.time() - start_time

            # Выводим результат
            if result.stdout:
                print(result.stdout[:2000])
            if result.stderr:
                print(result.stderr[:2000])

            if result.returncode != 0:
                print("❌ Import failed!")
                sys.exit(1)

            print(f"✅ Database restored successfully!")
            print(f"   Time: {elapsed:.1f} seconds")
            print()
            print("=" * 60)
            print("✅ IMPORT COMPLETED SUCCESSFULLY!")
            print("=" * 60)

            sys.exit(0)

    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()