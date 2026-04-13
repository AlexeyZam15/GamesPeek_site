#!/usr/bin/env python

"""
Export Django database to full PostgreSQL native dump with schema and data.
Usage: python export_db.py
"""

import os
import sys
import subprocess
import django
import shutil
import time
from pathlib import Path


def setup_django():
    """Setup Django environment."""
    PROJECT_ROOT = Path(__file__).parent
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'igdb_site.settings')
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    django.setup()


def find_pg_dump():
    """Find pg_dump executable in bundled PostgreSQL or system."""
    # Check bundled PostgreSQL first
    bundled_paths = [
        Path(__file__).parent / 'PostgreSQL' / '18' / 'bin' / 'pg_dump.exe',
        Path(__file__).parent / 'dist' / 'gamespeek' / 'PostgreSQL' / '18' / 'bin' / 'pg_dump.exe',
    ]

    for path in bundled_paths:
        if path.exists():
            print(f"  Using bundled PostgreSQL: {path}")
            return str(path)

    # Check system PostgreSQL
    system_paths = [
        r'C:\Program Files\PostgreSQL\18\bin\pg_dump.exe',
        r'C:\Program Files (x86)\PostgreSQL\18\bin\pg_dump.exe',
    ]

    for path in system_paths:
        if Path(path).exists():
            print(f"  Using system PostgreSQL: {path}")
            return path

    raise Exception("pg_dump not found! Please install PostgreSQL 18.1")


def export_database():
    """Export database to full PostgreSQL native dump."""
    print("=" * 60)
    print("📤 DATABASE EXPORTER (Full dump with schema)")
    print("=" * 60)
    print()

    setup_django()

    from django.conf import settings

    db_settings = settings.DATABASES['default']

    db_name = db_settings['NAME']
    db_user = db_settings['USER']
    db_host = db_settings.get('HOST', 'localhost')
    db_port = db_settings.get('PORT', '5432')
    db_password = db_settings.get('PASSWORD', '')

    print(f"📁 Database: {db_name}")
    print(f"👤 User: {db_user}")
    print(f"🔗 Host: {db_host}:{db_port}")
    print()

    project_root = Path(__file__).parent
    dump_file = project_root / 'database.dump'

    print(f"📁 Output: {dump_file.name}")
    print()

    try:
        pg_dump_path = find_pg_dump()
        print(f"🔧 Using pg_dump: {pg_dump_path}")
        print()
    except Exception as e:
        print(f"❌ {e}")
        return False

    cmd = [
        pg_dump_path,
        '--dbname', f'postgresql://{db_user}@{db_host}:{db_port}/{db_name}',
        '--format', 'custom',
        '--compress', '9',
        '--no-sync',
        '--verbose',
    ]

    env = os.environ.copy()
    if db_password:
        env['PGPASSWORD'] = db_password

    print("⏳ Exporting full database with schema and data...")
    print("   (This may take several minutes)")
    print()

    start_time = time.time()

    with open(dump_file, 'wb') as f:
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

    dump_size_mb = dump_file.stat().st_size / (1024 * 1024)
    print(f"✅ Full dump created: {dump_file.name}")
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
    print("Next steps:")
    print("1. database.dump is ready for distribution")
    print("2. The dump will be automatically imported when running the compiled app")

    return True


def main():
    try:
        success = export_database()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()