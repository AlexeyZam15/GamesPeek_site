#!/usr/bin/env python

"""
Build script for gamespeek.exe - маленький загрузчик, который запускает Python из venv
"""

import os
import sys
import shutil
import stat
import subprocess
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
DIST_DIR = PROJECT_ROOT / 'dist'
BUILD_DIR = PROJECT_ROOT / 'build'


def remove_readonly(func, path, excinfo):
    """Remove read-only attribute from files."""
    os.chmod(path, stat.S_IWRITE)
    func(path)


def play_sound(success=True):
    """Play sound notification when build completes."""
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


def clean_build_files():
    """Remove old build artifacts."""
    print("\n🗑️ Cleaning old build files...")

    if sys.platform == 'win32':
        try:
            subprocess.run('taskkill /F /IM gamespeek.exe', shell=True, capture_output=True)
            print("  ✅ Killed running gamespeek.exe")
            time.sleep(2)
        except:
            pass

    if DIST_DIR.exists():
        for attempt in range(3):
            try:
                shutil.rmtree(DIST_DIR, onerror=remove_readonly, ignore_errors=False)
                print("  ✅ Removed dist folder")
                break
            except Exception as e:
                print(f"  ⚠️ Attempt {attempt + 1} failed: {e}")
                time.sleep(2)
                if attempt == 2:
                    subprocess.run(f'rmdir /s /q "{DIST_DIR}"', shell=True)
                    print("  ✅ Force removed dist folder")

    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR, onerror=remove_readonly, ignore_errors=True)
        print("  ✅ Removed build folder")


def clean_database():
    """Delete PostgreSQL database folder."""
    if sys.platform == 'win32':
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
        return False
    else:
        print("✅ Static files collected successfully")

    return True


def copy_postgresql_binaries():
    """Copy PostgreSQL 18.1 folder with all extensions to build using multithreading."""
    import shutil
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    from tqdm import tqdm

    exe_dir = DIST_DIR / 'gamespeek'

    # Source PostgreSQL folder
    pg_source = Path(r'P:\Program Files\PostgreSQL\18')

    if not pg_source.exists():
        print(f"❌ PostgreSQL not found at {pg_source}")
        return False

    # Destination folder in build
    dest_pg_dir = exe_dir / 'PostgreSQL' / '18'

    print(f"📦 Copying PostgreSQL 18 from {pg_source}")
    print(f"   This may take a few minutes...")

    # Remove old if exists
    if dest_pg_dir.exists():
        shutil.rmtree(dest_pg_dir, onerror=remove_readonly, ignore_errors=True)

    # Collect all files to copy
    files_to_copy = []
    for item in pg_source.rglob('*'):
        if item.is_file():
            # Skip unnecessary folders
            if 'pgAdmin' in str(item) or 'docs' in str(item) or 'symbols' in str(item) or item.suffix == '.pdb':
                continue
            rel_path = item.relative_to(pg_source)
            dst_path = dest_pg_dir / rel_path
            files_to_copy.append((item, dst_path))

    total_files = len(files_to_copy)
    total_size_mb = sum(f[0].stat().st_size for f in files_to_copy) / (1024 * 1024)

    print(f"📦 Copying {total_files} files ({total_size_mb:.1f} MB) using 8 threads...")

    # Create directories
    unique_dirs = set()
    for _, dst_path in files_to_copy:
        parent = dst_path.parent
        if parent not in unique_dirs:
            unique_dirs.add(parent)
            parent.mkdir(parents=True, exist_ok=True)

    copied = 0
    lock = threading.Lock()
    start_time = time.time()

    def copy_file(src_dst):
        src, dst = src_dst
        try:
            shutil.copy2(src, dst)
            return True
        except Exception as e:
            return False

    # Use tqdm for progress bar
    with tqdm(total=total_files, desc="  Copying PostgreSQL", unit="files", ncols=80) as pbar:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(copy_file, (src, dst)): i for i, (src, dst) in enumerate(files_to_copy)}

            for future in as_completed(futures):
                with lock:
                    copied += 1
                    pbar.update(1)

                    if copied % 100 == 0 or copied == total_files:
                        elapsed = time.time() - start_time
                        percent = (copied / total_files) * 100
                        speed = copied / elapsed if elapsed > 0 else 0
                        pbar.set_postfix({
                            'files/s': f'{speed:.0f}',
                            'MB': f'{copied / total_files * total_size_mb:.1f}/{total_size_mb:.1f}'
                        })

    elapsed_total = time.time() - start_time
    print(f"\n  ✅ Copied {total_files} files in {elapsed_total:.1f} seconds")

    # Verify extensions were copied
    dest_extensions = dest_pg_dir / 'share' / 'extension'
    if dest_extensions.exists():
        btree_gin = dest_extensions / 'btree_gin.control'
        if btree_gin.exists():
            print(f"  ✅ btree_gin.control found")
        else:
            print(f"  ⚠️ btree_gin.control not found")

        control_count = len(list(dest_extensions.glob('*.control')))
        print(f"  ✅ {control_count} extensions copied")
    else:
        print(f"  ❌ Extensions directory not found")

    print(f"✅ PostgreSQL 18 copied to {dest_pg_dir}")
    return True


def create_loader_script():
    """Создает загрузчик loader.py с отладкой и увеличенным таймаутом."""
    loader_content = '''#!/usr/bin/env python

"""
Загрузчик gamespeek - запускает run.py через python из venv
"""

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
        except:
            print("ERROR: Virtual environment not found!")
        return 1

    if not run_script.exists():
        try:
            import tkinter as tk
            from tkinter import messagebox
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror("Ошибка", "run.py не найден!")
        except:
            print("ERROR: run.py not found!")
        return 1

    # Запускаем с подавлением окна
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
            startupinfo=startupinfo
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
    print(f"✅ Created loader.py")
    return loader_path


def build_loader():
    """Собирает загрузчик в exe (только один файл)."""
    print("\n" + "=" * 50)
    print("Building loader.exe")
    print("=" * 50)

    loader_path = create_loader_script()

    output_dir = DIST_DIR / 'gamespeek'
    output_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, '-m', 'PyInstaller',
        '--onefile',
        '--noconsole',
        '--name', 'gamespeek',
        '--noconfirm',
        '--distpath', str(output_dir),
        '--workpath', str(BUILD_DIR),
        '--specpath', str(PROJECT_ROOT),
        str(loader_path)
    ]

    result = subprocess.run(cmd, cwd=PROJECT_ROOT)

    if result.returncode != 0:
        print("\n❌ Loader build failed!")
        return False

    final_exe = output_dir / 'gamespeek.exe'
    if final_exe.exists():
        print(f"✅ Loader built: {final_exe}")
    else:
        print(f"❌ Loader not found at {final_exe}")
        return False

    print("\n✅ Loader built successfully")
    return True


def copy_venv_and_files():
    """Копирует venv и все необходимые файлы в dist."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    exe_dir = DIST_DIR / 'gamespeek'
    exe_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n📁 Copying files to: {exe_dir}")

    source_venv = PROJECT_ROOT.parent / 'venv'
    dest_venv = exe_dir / 'venv'
    if source_venv.exists():
        if dest_venv.exists():
            shutil.rmtree(dest_venv, onerror=remove_readonly, ignore_errors=True)

        print("📦 Preparing to copy venv...")
        files_to_copy = []
        ignore_patterns = {'__pycache__', '*.pyc', '*.pyo', 'tests', 'test', 'docs', 'doc'}

        for item in source_venv.rglob('*'):
            if item.is_file():
                should_ignore = False
                for pattern in ignore_patterns:
                    if pattern.startswith('*'):
                        if item.suffix == pattern[1:]:
                            should_ignore = True
                            break
                    elif pattern in item.parts:
                        should_ignore = True
                        break
                if not should_ignore:
                    rel_path = item.relative_to(source_venv)
                    dst_path = dest_venv / rel_path
                    files_to_copy.append((item, dst_path))

        total_files = len(files_to_copy)
        total_size_mb = sum(f[0].stat().st_size for f in files_to_copy) / (1024 * 1024)
        print(f"📦 Copying {total_files} files ({total_size_mb:.1f} MB) from venv using 8 threads...")

        unique_dirs = set()
        for _, dst_path in files_to_copy:
            parent = dst_path.parent
            if parent not in unique_dirs:
                unique_dirs.add(parent)
                parent.mkdir(parents=True, exist_ok=True)

        copied = 0
        lock = threading.Lock()
        start_time = time.time()

        def copy_file(src_dst):
            src, dst = src_dst
            try:
                shutil.copy2(src, dst)
                return True
            except Exception as e:
                return False

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(copy_file, (src, dst)): i for i, (src, dst) in enumerate(files_to_copy)}

            for future in as_completed(futures):
                with lock:
                    copied += 1

                    if copied % 100 == 0 or copied == total_files:
                        elapsed = time.time() - start_time
                        percent = (copied / total_files) * 100
                        speed = copied / elapsed
                        eta_seconds = (total_files - copied) / speed if speed > 0 else 0

                        if eta_seconds < 60:
                            eta_str = f"{eta_seconds:.0f} сек"
                        elif eta_seconds < 3600:
                            eta_str = f"{eta_seconds // 60:.0f} мин {eta_seconds % 60:.0f} сек"
                        else:
                            eta_str = f"{eta_seconds // 3600:.0f} ч {(eta_seconds % 3600) // 60:.0f} мин"

                        bar_length = 30
                        filled = int(bar_length * copied // total_files)
                        bar = '█' * filled + '░' * (bar_length - filled)

                        print(
                            f"\r   [{bar}] {copied}/{total_files} ({percent:.1f}%) | {elapsed:.0f}с прошло | осталось: {eta_str} | {speed:.0f} файлов/сек",
                            end='', flush=True)

        elapsed_total = time.time() - start_time
        print(f"\n✅ Copied venv ({total_files} files) за {elapsed_total:.1f} сек")
    else:
        print(f"❌ venv not found at {source_venv}")
        return False

    # Copy PostgreSQL 18.1 binaries (without pgembed)
    print("\n📦 Copying PostgreSQL 18.1 binaries...")
    copy_postgresql_binaries()

    def copy_item(src, dst, name):
        if src.exists():
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
        else:
            print(f"⚠️ {name} not found at {src}")

    copy_item(PROJECT_ROOT / 'run.py', exe_dir / 'run.py', 'run.py')
    copy_item(PROJECT_ROOT / 'igdb_site', exe_dir / 'igdb_site', 'igdb_site')
    copy_item(PROJECT_ROOT / 'games', exe_dir / 'games', 'games')
    copy_item(PROJECT_ROOT / 'staticfiles', exe_dir / 'staticfiles', 'staticfiles')

    dump_file = PROJECT_ROOT / 'database.dump'
    copy_item(dump_file, exe_dir / 'database.dump', 'database.dump')

    logs_dir = exe_dir / 'logs'
    logs_dir.mkdir(exist_ok=True)
    print(f"✅ Created logs folder")

    total_size = 0
    file_count = 0
    for item in exe_dir.rglob('*'):
        if item.is_file():
            total_size += item.stat().st_size
            file_count += 1

    print(f"\n📊 Copied {file_count} files, total size: {total_size // (1024 * 1024)} MB")

    return True


def main():
    print("=" * 60)
    print("🚀 gamespeek.exe BUILDER (venv loader mode)")
    print("=" * 60)
    print()

    build_success = True

    print("Step 0: Cleaning database...")
    clean_database()

    print("\nStep 1: Cleaning build files...")
    clean_build_files()

    print("\nStep 2: Collecting static files...")
    if not collect_static_files():
        build_success = False

    if build_success:
        print("\nStep 3: Building loader...")
        if not build_loader():
            build_success = False

    if build_success:
        print("\nStep 4: Copying venv and files...")
        if not copy_venv_and_files():
            build_success = False

    print("\n" + "=" * 60)
    if build_success:
        print("✅ BUILD COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"\n📁 All files are in: {DIST_DIR}/gamespeek/")
        print("   - gamespeek.exe (маленький загрузчик)")
        print("   - venv/ (виртуальное окружение с библиотеками)")
        print("   - database.dump (сжатый дамп базы данных)")
        print("   - PostgreSQL 18.1 binaries")
        print("   - run.py и все файлы проекта")
        print("\nTo run: double-click gamespeek.exe")
        play_sound(success=True)
    else:
        print("❌ BUILD FAILED!")
        print("=" * 60)
        play_sound(success=False)
        sys.exit(1)


if __name__ == '__main__':
    main()