#!/usr/bin/env python

"""
Build script for gamespeek.exe - маленький загрузчик, который запускает Python из venv.

Обновлено: конфигурация через env/argparse, проверки предусловий,
fallback без tqdm, копирование .env, безопасные фильтры копирования
PostgreSQL и venv (исключаем data, pgAdmin, installer, symbols, include,
тесты, .dist-info, PyInstaller, pip, playwright, selenium, googleapiclient,
pandas/numpy (опционально) и т.п., чтобы сборка была меньше и быстрее).
"""

import argparse
import fnmatch
import os
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
DIST_DIR = PROJECT_ROOT / 'dist'
BUILD_DIR = PROJECT_ROOT / 'build'


def remove_readonly(func, path, excinfo):
    """Remove read-only attribute from files."""
    os.chmod(path, stat.S_IWRITE)
    func(path)


def play_sound(success=True, enabled=True):
    """Play sound notification when build completes."""
    if not enabled:
        return
    try:
        if sys.platform == 'win32':
            import winsound
            if success:
                winsound.Beep(800, 500)
                winsound.Beep(1000, 300)
            else:
                winsound.Beep(400, 300)
                winsound.Beep(300, 300)
        else:
            print('\a' * 3, end='', flush=True)
    except Exception as e:
        print(f"  Could not play sound: {e}")


def _progress(copied, total, start_time, prefix=''):
    """Ручной прогресс-бар (fallback без tqdm)."""
    if total <= 0:
        return
    elapsed = max(time.time() - start_time, 1e-6)
    percent = (copied / total) * 100
    speed = copied / elapsed
    eta_seconds = (total - copied) / speed if speed > 0 else 0
    if eta_seconds < 60:
        eta_str = f"{eta_seconds:.0f} сек"
    elif eta_seconds < 3600:
        eta_str = f"{eta_seconds // 60:.0f} мин {eta_seconds % 60:.0f} сек"
    else:
        eta_str = f"{eta_seconds // 3600:.0f} ч {(eta_seconds % 3600) // 60:.0f} мин"
    bar_length = 30
    filled = int(bar_length * copied // total)
    bar = '█' * filled + '░' * (bar_length - filled)
    print(
        f"\r   {prefix}[{bar}] {copied}/{total} ({percent:.1f}%) | "
        f"{elapsed:.0f}с прошло | осталось: {eta_str} | {speed:.0f} файлов/сек",
        end='', flush=True,
    )


def _resolve_paths(args):
    """Резолвит пути из аргументов/env с фолбэками."""
    pg_source = Path(
        args.pg_source
        or os.environ.get('PG_SOURCE_DIR')
        or r'P:\Program Files\PostgreSQL\18'
    )

    if args.venv:
        venv = Path(args.venv)
    elif os.environ.get('VENV_DIR'):
        venv = Path(os.environ['VENV_DIR'])
    else:
        candidates = [PROJECT_ROOT.parent / 'venv', PROJECT_ROOT / 'venv']
        venv = next((c for c in candidates if c.exists()), candidates[0])

    dump = Path(
        args.database_dump
        or os.environ.get('DATABASE_DUMP')
        or (PROJECT_ROOT / 'database.dump')
    )

    return pg_source, venv, dump


def check_prerequisites(args, pg_source, venv, dump):
    """Проверяет предусловия сборки. Возвращает True если всё ок."""
    ok = True

    if sys.version_info < (3, 10):
        print(f"❌ Python 3.10+ required, got {sys.version_info.major}.{sys.version_info.minor}")
        ok = False

    try:
        import PyInstaller  # noqa: F401
    except ImportError:
        print("❌ PyInstaller not installed. Run: pip install pyinstaller")
        ok = False

    if not (PROJECT_ROOT / 'run.py').exists():
        print(f"❌ run.py not found at {PROJECT_ROOT / 'run.py'}")
        ok = False

    if not (PROJECT_ROOT / 'manage.py').exists():
        print(f"❌ manage.py not found at {PROJECT_ROOT / 'manage.py'}")
        ok = False

    if not venv.exists():
        print(f"❌ venv not found at {venv}. Set VENV_DIR env or --venv.")
        ok = False
    elif not (venv / 'Scripts' / 'python.exe').exists():
        print(f"❌ venv python.exe not found at {venv / 'Scripts' / 'python.exe'}")
        ok = False

    if not pg_source.exists() and not args.skip_postgres:
        print(f"❌ PostgreSQL not found at {pg_source}. Set PG_SOURCE_DIR env or --pg-source.")
        ok = False

    if not dump.exists():
        if args.require_dump:
            print(f"❌ database.dump not found at {dump} (--require-dump).")
            ok = False
        else:
            print(f"⚠️ database.dump not found at {dump}. Will be skipped.")

    robots_dir = PROJECT_ROOT / 'robots_txt'
    if not robots_dir.exists():
        robots_dir.mkdir(parents=True, exist_ok=True)
        print(f"✅ Created empty {robots_dir} (needed for collectstatic)")

    return ok


def list_big_dirs(root, top_n=30, subdir=None):
    """Печатает топ-N самых больших каталогов внутри root (рекурсивно).

    Если subdir задан — сканирует root/subdir (например, venv/Lib/site-packages).
    Группирует по первому уровню внутри scan_root.
    """
    scan_root = root / subdir if subdir else root
    if not scan_root.exists():
        print(f"  ⚠️ {scan_root} not found")
        return

    sizes = {}
    try:
        for item in scan_root.rglob('*'):
            if not item.is_file():
                continue
            try:
                rel = item.relative_to(scan_root)
                first = rel.parts[0] if rel.parts else '.'
                sizes[first] = sizes.get(first, 0) + item.stat().st_size
            except Exception:
                continue
    except Exception as e:
        print(f"  ⚠️ Error scanning {scan_root}: {e}")
        return

    top = sorted(sizes.items(), key=lambda x: -x[1])[:top_n]
    print(f"\n  Top-{top_n} biggest entries in {scan_root}:")
    for name, size in top:
        print(f"    {name:45s} {size / (1024 * 1024):10.1f} MB")


def clean_build_files():
    """Remove old build artifacts."""
    print("\n🗑️ Cleaning old build files...")

    if sys.platform == 'win32':
        subprocess.run('taskkill /F /IM gamespeek.exe', shell=True, capture_output=True, check=False)
        print("  ✅ taskkill executed (if process existed)")
        time.sleep(1)

    if DIST_DIR.exists():
        for attempt in range(3):
            try:
                shutil.rmtree(DIST_DIR, onerror=remove_readonly, ignore_errors=True)
                if not DIST_DIR.exists():
                    print("  ✅ Removed dist folder")
                    break
            except Exception as e:
                print(f"  ⚠️ Attempt {attempt + 1} failed: {e}")
                time.sleep(2)
        else:
            subprocess.run(f'rmdir /s /q "{DIST_DIR}"', shell=True, check=False)
            print("  ✅ Force removed dist folder (rmdir)")

    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR, onerror=remove_readonly, ignore_errors=True)
        print("  ✅ Removed build folder")


def clean_database():
    """Delete PostgreSQL database folder (data dir)."""
    data_dir_env = os.environ.get('DESKTOP_DATA_DIR')
    if data_dir_env:
        data_dir = Path(data_dir_env)
    elif sys.platform == 'win32':
        appdata = os.environ.get('APPDATA', os.path.expanduser('~'))
        data_dir = Path(appdata) / 'IGDB_Site_PostgreSQL'
    else:
        data_dir = Path.home() / '.igdb_site_postgresql'

    if data_dir.exists():
        print(f"🗑️ Cleaning database at: {data_dir}")
        shutil.rmtree(data_dir, onerror=remove_readonly, ignore_errors=True)
        print("✅ Database cleaned")
    else:
        print("ℹ️ No database to clean")


def collect_static_files():
    """Run collectstatic to gather all static files."""
    print("\n📦 Collecting static files...")

    static_root = PROJECT_ROOT / 'staticfiles'
    if static_root.exists():
        shutil.rmtree(static_root, onerror=remove_readonly, ignore_errors=True)
        print("  ✅ Removed old staticfiles folder")

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'igdb_site.settings')

    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    manage_py = PROJECT_ROOT / 'manage.py'
    if manage_py.exists():
        cmd = [
            sys.executable, str(manage_py), 'collectstatic',
            '--noinput', '--clear', '--verbosity=1',
        ]
    else:
        cmd = [
            sys.executable, '-m', 'django', 'collectstatic',
            '--noinput', '--clear', '--settings=igdb_site.settings',
        ]

    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)

    if result.returncode != 0:
        print("⚠️ collectstatic failed!")
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        return False

    if not static_root.exists() or not any(static_root.iterdir()):
        print(f"❌ collectstatic produced empty {static_root}")
        return False

    print("✅ Static files collected successfully")
    return True


def copy_postgresql_binaries(pg_source):
    """Copy PostgreSQL binaries to build, using path-based filters.

    Исключаем тяжёлые и ненужные для runtime каталоги и файлы:
    - data (сама БД; run.py создаёт свою в AppData)
    - pgAdmin / pgAdmin 4 / docs / doc / symbols / include / stackbuilder
    - installer
    - share/doc, share/man, share/locale
    - *.pdb, *.lib, uninstall-*, *_license.txt, *_3rd_party_licenses.txt,
      installation_summary.log, pg_env.bat
    """
    exe_dir = DIST_DIR / 'gamespeek'
    dest_pg_dir = exe_dir / 'PostgreSQL' / pg_source.name

    if not pg_source.exists():
        print(f"⚠️ PostgreSQL not found at {pg_source}, skipping")
        return True

    print(f"📦 Copying PostgreSQL from {pg_source}")

    if dest_pg_dir.exists():
        shutil.rmtree(dest_pg_dir, onerror=remove_readonly, ignore_errors=True)

    exclude_dirs = {
        'data',
        'pgadmin', 'pgadmin4', 'pgadmin 4',
        'docs', 'doc',
        'symbols', 'include',
        'stackbuilder', 'stack builder',
        'installer',
    }
    exclude_share_dirs = {'doc', 'man', 'locale'}

    exclude_patterns = [
        '*.pdb', '*.lib',
        'uninstall-postgresql.exe', 'uninstall-postgresql.dat',
        'uninstall-*',
        '*_license.txt', '*_3rd_party_licenses.txt',
        'installation_summary.log', 'pg_env.bat',
    ]

    files_to_copy = []
    for item in pg_source.rglob('*'):
        if not item.is_file():
            continue
        rel = item.relative_to(pg_source)
        rel_parts = [p.lower() for p in rel.parts]

        if any(part in exclude_dirs for part in rel_parts):
            continue

        if len(rel_parts) >= 2 and rel_parts[0] == 'share' and rel_parts[1] in exclude_share_dirs:
            continue

        if any(fnmatch.fnmatch(item.name.lower(), pat) for pat in exclude_patterns):
            continue

        files_to_copy.append((item, dest_pg_dir / rel))

    total_files = len(files_to_copy)
    total_size_mb = sum(f[0].stat().st_size for f in files_to_copy) / (1024 * 1024)
    print(f"📦 Copying {total_files} files ({total_size_mb:.1f} MB) from PostgreSQL...")

    for _, dst in files_to_copy:
        dst.parent.mkdir(parents=True, exist_ok=True)

    try:
        from tqdm import tqdm
        use_tqdm = True
    except ImportError:
        use_tqdm = False

    copied = 0
    start_time = time.time()

    iterator = tqdm(total=total_files, desc="  Copying PostgreSQL", unit="files", ncols=80) if use_tqdm else None
    for src, dst in files_to_copy:
        try:
            shutil.copy2(src, dst)
        except Exception:
            pass
        copied += 1
        if use_tqdm:
            iterator.update(1)
        elif copied % 100 == 0 or copied == total_files:
            _progress(copied, total_files, start_time, prefix='PostgreSQL ')

    if use_tqdm:
        iterator.close()
    else:
        print()

    elapsed = time.time() - start_time
    print(f"  ✅ Copied {total_files} files in {elapsed:.1f} seconds")

    dest_extensions = dest_pg_dir / 'share' / 'extension'
    if dest_extensions.exists():
        control_count = len(list(dest_extensions.glob('*.control')))
        btree_gin = (dest_extensions / 'btree_gin.control').exists()
        print(f"  ℹ️ {control_count} extensions copied (btree_gin: {btree_gin})")

    print(f"✅ PostgreSQL copied to {dest_pg_dir}")
    return True


def create_loader_script():
    """Создает загрузчик loader.py."""
    loader_content = '''#!/usr/bin/env python

"""Загрузчик gamespeek - запускает run.py через python из venv"""

import subprocess
import os
import sys
from pathlib import Path


def main():
    if getattr(sys, 'frozen', False):
        base_path = Path(sys.executable).parent
    else:
        base_path = Path(__file__).parent

    venv_python = base_path / 'venv' / 'Scripts' / 'python.exe'
    run_script = base_path / 'run.py'

    if not venv_python.exists():
        try:
            import tkinter as tk
            from tkinter import messagebox
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror("Ошибка", "Виртуальное окружение не найдено!")
        except Exception:
            print("ERROR: Virtual environment not found!")
        return 1

    if not run_script.exists():
        try:
            import tkinter as tk
            from tkinter import messagebox
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror("Ошибка", "run.py не найден!")
        except Exception:
            print("ERROR: run.py not found!")
        return 1

    startupinfo = None
    creationflags = 0

    if sys.platform == 'win32':
        creationflags = 0x08000000  # CREATE_NO_WINDOW
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags = subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = 0

    try:
        process = subprocess.Popen(
            [str(venv_python), str(run_script)],
            creationflags=creationflags,
            startupinfo=startupinfo,
        )
        process.wait()
    except Exception as e:
        print(f"ERROR: {e}")
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
'''

    loader_path = PROJECT_ROOT / 'loader.py'
    with open(loader_path, 'w', encoding='utf-8') as f:
        f.write(loader_content)
    print("✅ Created loader.py")
    return loader_path


def build_loader():
    """Собирает загрузчик в exe."""
    print("\n" + "=" * 50)
    print("Building loader.exe")
    print("=" * 50)

    loader_path = create_loader_script()

    output_dir = DIST_DIR / 'gamespeek'
    output_dir.mkdir(parents=True, exist_ok=True)
    BUILD_DIR.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, '-m', 'PyInstaller',
        '--onefile',
        '--noconsole',
        '--name', 'gamespeek',
        '--noconfirm',
        '--distpath', str(output_dir),
        '--workpath', str(BUILD_DIR),
        '--specpath', str(BUILD_DIR),
        str(loader_path),
    ]

    result = subprocess.run(cmd, cwd=PROJECT_ROOT)

    if result.returncode != 0:
        print("\n❌ Loader build failed!")
        return False

    final_exe = output_dir / 'gamespeek.exe'
    if final_exe.exists():
        print(f"✅ Loader built: {final_exe}")
        return True

    print(f"❌ Loader not found at {final_exe}")
    return False


def _venv_exclude(rel_parts, item, strip_data_libs=False):
    """Возвращает True, если файл venv надо исключить."""
    # Кэши Python
    if '__pycache__' in rel_parts:
        return True
    if item.suffix.lower() in ('.pyc', '.pyo'):
        return True

    # Тесты пакетов
    if any(part in ('tests', 'test', 'testing') for part in rel_parts):
        return True

    # Метаданные пакетов
    if item.suffix.lower() in ('.dist-info', '.egg-info'):
        return True

    # Документация пакетов
    if any(part in ('docs', 'doc') for part in rel_parts):
        return True

    # Инструменты сборки и лишние пакеты (не нужны в runtime)
    always_exclude = {
        'pyinstaller', '_pyinstaller_hooks_contrib', 'pyinstaller_hooks_contrib',
        'altgraph', 'pefile', 'pywin32_ctypes',
        'pip', 'wheel',
        'debug_toolbar',
        'playwright', 'selenium', 'googleapiclient', 'google',
        'winpty', 'wcwidth', 'trio', '_pytest', 'aiohttp',
        'openpyxl', 'joblib', 'pgembed',
    }
    if any(part in always_exclude for part in rel_parts):
        return True

    # Опционально: тяжёлые data-библиотеки (pandas/numpy).
    # Могут использоваться страницами analyze в админке.
    if strip_data_libs:
        data_libs = {
            'pandas', 'numpy', '~umpy',
            'numpy.libs', '~umpy.libs',
        }
        if any(part in data_libs for part in rel_parts):
            return True

    return False


def copy_venv_and_files(venv, dump, args):
    """Копирует venv и все необходимые файлы в dist.

    Фильтр venv: исключаем __pycache__, *.pyc/*.pyo, tests/test/testing,
    *.dist-info, *.egg-info, docs/doc, PyInstaller/pip/wheel/debug_toolbar,
    playwright/selenium/googleapiclient/pandas/numpy (опционально) и т.п.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    exe_dir = DIST_DIR / 'gamespeek'
    exe_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n📁 Copying files to: {exe_dir}")

    # ===== venv =====
    dest_venv = exe_dir / 'venv'
    if venv.exists():
        if dest_venv.exists():
            shutil.rmtree(dest_venv, onerror=remove_readonly, ignore_errors=True)

        print("📦 Preparing to copy venv...")
        files_to_copy = []
        for item in venv.rglob('*'):
            if not item.is_file():
                continue
            rel = item.relative_to(venv)
            rel_parts = [p.lower() for p in rel.parts]

            if _venv_exclude(rel_parts, item, strip_data_libs=args.strip_data_libs):
                continue

            files_to_copy.append((item, dest_venv / rel))

        total_files = len(files_to_copy)
        total_size_mb = sum(f[0].stat().st_size for f in files_to_copy) / (1024 * 1024)
        print(f"📦 Copying {total_files} files ({total_size_mb:.1f} MB) from venv using 8 threads...")

        for _, dst in files_to_copy:
            dst.parent.mkdir(parents=True, exist_ok=True)

        copied = 0
        lock = threading.Lock()
        start_time = time.time()

        def copy_file(src_dst):
            src, dst = src_dst
            try:
                shutil.copy2(src, dst)
                return True
            except Exception:
                return False

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(copy_file, (src, dst)) for src, dst in files_to_copy]
            for _ in as_completed(futures):
                with lock:
                    copied += 1
                    if copied % 100 == 0 or copied == total_files:
                        _progress(copied, total_files, start_time, prefix='venv ')

        print()
        elapsed_total = time.time() - start_time
        print(f"✅ Copied venv ({total_files} files) за {elapsed_total:.1f} сек")
    else:
        print(f"❌ venv not found at {venv}")
        return False

    # ===== PostgreSQL =====
    if not args.skip_postgres:
        print("\n📦 Copying PostgreSQL binaries...")
        copy_postgresql_binaries(args.pg_source_resolved)

    # ===== project files =====
    def copy_item(src, dst, name):
        if not src.exists():
            print(f"⚠️ {name} not found at {src}")
            return
        if dst.exists():
            if dst.is_dir():
                shutil.rmtree(dst, onerror=remove_readonly, ignore_errors=True)
            else:
                dst.unlink()
        if src.is_dir():
            shutil.copytree(src, dst, ignore=shutil.ignore_patterns('*.pyc', '__pycache__'))
        else:
            shutil.copy2(src, dst)
        print(f"✅ Copied {name}")

    copy_item(PROJECT_ROOT / 'run.py', exe_dir / 'run.py', 'run.py')
    copy_item(PROJECT_ROOT / 'igdb_site', exe_dir / 'igdb_site', 'igdb_site')
    copy_item(PROJECT_ROOT / 'games', exe_dir / 'games', 'games')
    copy_item(PROJECT_ROOT / 'staticfiles', exe_dir / 'staticfiles', 'staticfiles')

    if dump.exists():
        copy_item(dump, exe_dir / 'database.dump', 'database.dump')
    else:
        print(f"⚠️ database.dump not found at {dump}, skipped")

    env_file = PROJECT_ROOT / '.env'
    if env_file.exists():
        copy_item(env_file, exe_dir / '.env', '.env')
    else:
        print(f"⚠️ .env not found at {env_file}, skipped (run.py must create it)")

    logs_dir = exe_dir / 'logs'
    logs_dir.mkdir(exist_ok=True)
    print("✅ Created logs folder")

    total_size = 0
    file_count = 0
    for item in exe_dir.rglob('*'):
        if item.is_file():
            total_size += item.stat().st_size
            file_count += 1

    print(f"\n📊 Copied {file_count} files, total size: {total_size // (1024 * 1024)} MB")
    return True


def parse_args():
    parser = argparse.ArgumentParser(description='Build gamespeek.exe')
    parser.add_argument('--skip-db-clean', action='store_true', help='Do not delete existing desktop DB')
    parser.add_argument('--skip-static', action='store_true', help='Do not run collectstatic')
    parser.add_argument('--skip-postgres', action='store_true', help='Do not copy PostgreSQL binaries')
    parser.add_argument('--require-dump', action='store_true', help='Fail if database.dump missing')
    parser.add_argument('--pg-source', default=None, help='Path to PostgreSQL install dir')
    parser.add_argument('--venv', default=None, help='Path to venv dir')
    parser.add_argument('--database-dump', default=None, help='Path to database.dump')
    parser.add_argument('--no-sound', action='store_true', help='Disable build sound')
    parser.add_argument('--list-big-dirs', action='store_true',
                        help='Print top-30 biggest dirs in venv/Lib/site-packages and PostgreSQL')
    parser.add_argument('--strip-data-libs', action='store_true',
                        help='Also exclude pandas/numpy from venv (may break admin analyze pages)')
    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 60)
    print("🚀 gamespeek.exe BUILDER (venv loader mode)")
    print("=" * 60)
    print()

    pg_source, venv, dump = _resolve_paths(args)
    args.pg_source_resolved = pg_source

    print(f"  PG_SOURCE_DIR : {pg_source}")
    print(f"  VENV_DIR      : {venv}")
    print(f"  DATABASE_DUMP : {dump}")
    print()

    if args.list_big_dirs:
        print("Diagnostics: biggest dirs")
        list_big_dirs(venv, subdir='Lib/site-packages')
        if not args.skip_postgres:
            list_big_dirs(pg_source)
        print()
        sys.exit(0)

    if not check_prerequisites(args, pg_source, venv, dump):
        print("❌ Prerequisites failed. Aborting.")
        play_sound(success=False, enabled=not args.no_sound)
        sys.exit(1)

    if args.strip_data_libs:
        print("⚠️ --strip-data-libs: pandas/numpy будут исключены из venv.")
        print("   Страницы analyze в админке могут упасть с ImportError.")
        print()

    if not args.skip_db_clean:
        print("Step 0: Cleaning database...")
        clean_database()

    print("\nStep 1: Cleaning build files...")
    clean_build_files()

    build_success = True

    if not args.skip_static:
        print("\nStep 2: Collecting static files...")
        if not collect_static_files():
            build_success = False

    if build_success:
        print("\nStep 3: Building loader...")
        if not build_loader():
            build_success = False

    if build_success:
        print("\nStep 4: Copying venv and files...")
        if not copy_venv_and_files(venv, dump, args):
            build_success = False

    print("\n" + "=" * 60)
    if build_success:
        print("✅ BUILD COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"\n📁 All files are in: {DIST_DIR}/gamespeek/")
        print("   - gamespeek.exe (маленький загрузчик)")
        print("   - venv/ (без тестов, .dist-info, PyInstaller, pip, playwright, selenium,")
        print("            googleapiclient, pandas/numpy (если --strip-data-libs))")
        print("   - database.dump (если был найден)")
        print("   - PostgreSQL binaries (без data, pgAdmin, installer, symbols)")
        print("   - run.py и все файлы проекта")
        print("   - .env (если был в корне проекта)")
        print("\nTo run: double-click gamespeek.exe")
        play_sound(success=True, enabled=not args.no_sound)
    else:
        print("❌ BUILD FAILED!")
        print("=" * 60)
        play_sound(success=False, enabled=not args.no_sound)
        sys.exit(1)


if __name__ == '__main__':
    main()