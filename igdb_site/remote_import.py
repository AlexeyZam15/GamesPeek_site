#!/usr/bin/env python

"""
Remote database import utility for running ON Render server.
This script runs inside the Render environment where PostgreSQL client is available.
"""

import os
import subprocess
from pathlib import Path

def import_database_on_render():
    """Import database dump using system pg_restore on Render."""

    # Путь к дампу в репозитории Render
    dump_path = Path('/opt/render/project/src/database.dump')

    if not dump_path.exists():
        print(f"❌ Dump not found at {dump_path}")
        print("   Make sure database.dump is committed to GitHub")
        return False

    dump_size_mb = dump_path.stat().st_size / (1024 * 1024)
    print(f"📦 Dump file: {dump_path.name} ({dump_size_mb:.1f} MB)")

    # Строка подключения к базе (из переменных окружения Render)
    database_url = os.environ.get('DATABASE_URL')

    if not database_url:
        print("❌ DATABASE_URL environment variable not set")
        return False

    print(f"🔗 Connecting to: {database_url.split('@')[1] if '@' in database_url else 'database'}")
    print(f"🗜️ Importing...")

    # Используем системный pg_restore (есть на Render)
    restore_cmd = [
        'pg_restore',
        '--dbname', database_url,
        '--no-owner',
        '--no-privileges',
        '--jobs', '2',
        '--verbose',
        str(dump_path)
    ]

    try:
        result = subprocess.run(restore_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"❌ Import failed: {result.stderr[:500]}")
            return False

        print("✅ Import completed successfully")
        return True

    except Exception as e:
        print(f"❌ Failed: {e}")
        return False

if __name__ == '__main__':
    import_database_on_render()