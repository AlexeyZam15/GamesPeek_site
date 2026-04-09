#!/usr/bin/env python
"""
Build script for IGDB_Site.exe
"""

import os
import sys
import shutil
import stat
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
DIST_DIR = PROJECT_ROOT / 'dist'
BUILD_DIR = PROJECT_ROOT / 'build'


def remove_readonly(func, path, excinfo):
    """Remove read-only attribute from files."""
    os.chmod(path, stat.S_IWRITE)
    func(path)


def clean_database():
    """Delete PostgreSQL database folder."""
    if sys.platform == 'win32':
        appdata = os.environ.get('APPDATA', os.path.expanduser('~'))
        data_dir = Path(appdata) / 'IGDB_Site_PostgreSQL'
    else:
        data_dir = Path.home() / '.igdb_site_postgresql'

    if data_dir.exists():
        print(f"🗑️ Cleaning database at: {data_dir}")
        try:
            shutil.rmtree(data_dir, onerror=remove_readonly, ignore_errors=False)
            print("✅ Database cleaned successfully")
        except Exception as e:
            print(f"⚠️ Could not clean database: {e}")
    else:
        print("ℹ️ No database to clean")


def clean_build_files():
    """Remove old build artifacts."""
    print("\n🗑️ Cleaning old build files...")

    if DIST_DIR.exists():
        shutil.rmtree(DIST_DIR, onerror=remove_readonly, ignore_errors=True)
        print("  ✅ Removed dist folder")

    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR, onerror=remove_readonly, ignore_errors=True)
        print("  ✅ Removed build folder")


def collect_static_files():
    """Run collectstatic to gather all static files before building."""
    print("\n📦 Collecting static files...")

    static_root = PROJECT_ROOT / 'staticfiles'
    if static_root.exists():
        shutil.rmtree(static_root, onerror=remove_readonly, ignore_errors=True)
        print("  ✅ Removed old staticfiles folder")

    # Устанавливаем переменные окружения для корректной работы Django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'igdb_site.settings')

    # Добавляем путь проекта в sys.path
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    # Запускаем collectstatic через manage.py
    manage_py = PROJECT_ROOT / 'manage.py'
    if manage_py.exists():
        cmd = [
            sys.executable, str(manage_py), 'collectstatic',
            '--noinput',
            '--clear',
            '--verbosity=1'
        ]
    else:
        cmd = [
            sys.executable, '-m', 'django', 'collectstatic',
            '--noinput',
            '--clear',
            '--settings=igdb_site.settings'
        ]

    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)

    if result.returncode != 0:
        print("⚠️ collectstatic failed!")
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        sys.exit(1)
    else:
        print("✅ Static files collected successfully")
        print(result.stdout)

    # Проверяем, что папка создалась и в ней есть файлы
    if static_root.exists():
        all_files = list(static_root.rglob('*'))
        css_files = list(static_root.rglob('*.css'))
        js_files = list(static_root.rglob('*.js'))
        print(f"  ✅ staticfiles created: {len(all_files)} total files")
        print(f"     - CSS: {len(css_files)} files")
        print(f"     - JS: {len(js_files)} files")

        # Проверяем структуру
        admin_static = static_root / 'admin'
        games_static = static_root / 'games'
        if admin_static.exists():
            print(f"     - admin static: OK")
        if games_static.exists():
            print(f"     - games static: OK")
    else:
        print("  ❌ staticfiles folder was not created!")
        sys.exit(1)


def run_pyinstaller():
    """Execute pyinstaller to build gamespeek.exe."""
    print("\n" + "=" * 50)
    print("Building gamespeek.exe")
    print("=" * 50)

    spec_file = PROJECT_ROOT / 'IGDB_Site.spec'

    if not spec_file.exists():
        print("❌ IGDB_Site.spec not found!")
        sys.exit(1)

    cmd = [
        sys.executable, '-m', 'PyInstaller',
        '--noconfirm',
        str(spec_file)
    ]

    result = subprocess.run(cmd, cwd=PROJECT_ROOT)

    if result.returncode != 0:
        print("\n❌ PyInstaller failed!")
        sys.exit(1)

    print("\n✅ PyInstaller completed successfully")


def copy_all_to_dist():
    """Copy all needed files to the same folder as .exe."""
    # exe лежит прямо в dist/
    exe_dir = DIST_DIR

    print(f"\n📁 Copying files to: {exe_dir}")

    # 1. Копируем data.json
    source_data = PROJECT_ROOT / 'data.json'
    if source_data.exists():
        dest_data = exe_dir / 'data.json'
        shutil.copy2(source_data, dest_data)
        print(f"✅ Copied data.json to {dest_data}")
    else:
        print("⚠️ data.json not found in project root")

    # 2. Копируем статические файлы из staticfiles
    source_static = PROJECT_ROOT / 'staticfiles'
    dest_static = exe_dir / 'staticfiles'

    if source_static.exists() and source_static.is_dir():
        if dest_static.exists():
            shutil.rmtree(dest_static, onerror=remove_readonly, ignore_errors=True)

        shutil.copytree(source_static, dest_static)

        all_files = len(list(dest_static.rglob('*')))
        css_files = len(list(dest_static.rglob('*.css')))
        js_files = len(list(dest_static.rglob('*.js')))

        print(f"✅ Copied staticfiles to {dest_static}")
        print(f"   - Total: {all_files} files")
        print(f"   - CSS: {css_files} files")
        print(f"   - JS: {js_files} files")
    else:
        print("❌ staticfiles not found! Run collectstatic first.")
        sys.exit(1)

    # 3. Копируем папку igdb_site с настройками
    source_settings = PROJECT_ROOT / 'igdb_site'
    dest_settings = exe_dir / 'igdb_site'
    if source_settings.exists():
        if dest_settings.exists():
            shutil.rmtree(dest_settings, onerror=remove_readonly, ignore_errors=True)
        shutil.copytree(source_settings, dest_settings, ignore=shutil.ignore_patterns('*.pyc', '__pycache__'))
        print(f"✅ Copied igdb_site settings to {dest_settings}")

    # 4. Копируем файлы шаблонов
    source_templates = PROJECT_ROOT / 'games' / 'templates'
    dest_templates = exe_dir / 'games' / 'templates'
    if source_templates.exists():
        if dest_templates.exists():
            shutil.rmtree(dest_templates, onerror=remove_readonly, ignore_errors=True)
        dest_templates.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source_templates, dest_templates)
        print(f"✅ Copied templates to {dest_templates}")

    # 5. Копируем папку desktop_migrations
    source_migrations = PROJECT_ROOT / 'desktop_migrations'
    dest_migrations = exe_dir / 'desktop_migrations'
    if source_migrations.exists():
        if dest_migrations.exists():
            shutil.rmtree(dest_migrations, onerror=remove_readonly, ignore_errors=True)
        shutil.copytree(source_migrations, dest_migrations, ignore=shutil.ignore_patterns('*.pyc', '__pycache__'))
        print(f"✅ Copied desktop_migrations to {dest_migrations}")

    # 6. Копируем папку games
    source_games = PROJECT_ROOT / 'games'
    dest_games = exe_dir / 'games'
    if source_games.exists():
        if dest_games.exists():
            shutil.rmtree(dest_games, onerror=remove_readonly, ignore_errors=True)
        shutil.copytree(source_games, dest_games, ignore=shutil.ignore_patterns('*.pyc', '__pycache__', 'migrations'))
        print(f"✅ Copied games to {dest_games}")

    print(f"\n📁 Final structure in {exe_dir}:")
    for item in sorted(exe_dir.iterdir()):
        if item.is_dir():
            print(f"   📁 {item.name}/")
        else:
            print(f"   📄 {item.name}")


def main():
    print("=" * 60)
    print("🚀 gamespeek.exe BUILDER")
    print("=" * 60)
    print()

    print("Step 1: Cleaning database...")
    clean_database()

    print("\nStep 2: Cleaning build files...")
    clean_build_files()

    print("\nStep 3: Collecting static files...")
    collect_static_files()

    print("\nStep 4: Running PyInstaller...")
    run_pyinstaller()

    print("\nStep 5: Copying files to dist...")
    copy_all_to_dist()

    print("\n" + "=" * 60)
    print("✅ BUILD COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print(f"\n📁 All files are in: {DIST_DIR}")
    print("   - gamespeek.exe")
    print("   - data.json")
    print("   - staticfiles/ (with correct structure)")
    print("\nTo run: double-click gamespeek.exe")


if __name__ == '__main__':
    main()