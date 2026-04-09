# -*- mode: python ; coding: utf-8 -*-

import os
import sys
from pathlib import Path

# ============================================
# ПУТИ К ВАШЕМУ ПРОЕКТУ
# ============================================
project_root = 'P:/Users/Alexey/Desktop/igdb_site/igdb_site'
settings_path = 'P:/Users/Alexey/Desktop/igdb_site/igdb_site/igdb_site'
venv_site_packages = 'P:/Users/Alexey/Desktop/igdb_site/venv/Lib/site-packages'

# Путь к папке pgembed
pgembed_path = Path(venv_site_packages) / 'pgembed'
pgembed_full_path = str(pgembed_path)

# Добавляем пути для импорта
sys.path.insert(0, project_root)
sys.path.insert(0, settings_path)
sys.path.insert(0, venv_site_packages)

# Указываем Django настройки
os.environ['DJANGO_SETTINGS_MODULE'] = 'igdb_site.settings'

# ============================================
# СБОР ДАННЫХ ДЛЯ ВКЛЮЧЕНИЯ В .EXE
# ============================================
datas_list = []

# Копируем папки целиком
folders_to_copy = [
    ('games/templates', 'games/templates'),
    ('games/static', 'games/static'),
    ('games/templatetags', 'games/templatetags'),
    ('igdb_site', 'igdb_site'),
    ('desktop_migrations', 'desktop_migrations'),
]

for src, dst in folders_to_copy:
    src_path = os.path.join(project_root, src)
    if os.path.exists(src_path):
        datas_list.append((src_path, dst))
        print(f"[SPEC] Added folder: {src} -> {dst}")

# Добавляем pgembed
if os.path.exists(pgembed_full_path):
    datas_list.append((pgembed_full_path, 'pgembed'))
    print(f"[SPEC] Added pgembed: {pgembed_full_path}")

# Добавляем статические файлы из staticfiles если они уже собраны
staticfiles_path = Path(project_root) / 'staticfiles'
if staticfiles_path.exists():
    datas_list.append((str(staticfiles_path), 'staticfiles'))
    print(f"[SPEC] Added staticfiles: {staticfiles_path}")

# ============================================
# АНАЛИЗ ЗАВИСИМОСТЕЙ
# ============================================
a = Analysis(
    ['run.py'],
    pathex=[project_root, settings_path, venv_site_packages],
    binaries=[],
    datas=datas_list,
    hiddenimports=[
        'pgembed',
        'django',
        'django.contrib.admin',
        'django.contrib.auth',
        'django.contrib.contenttypes',
        'django.contrib.sessions',
        'django.contrib.messages',
        'django.contrib.staticfiles',
        'games',
        'games.models',
        'games.views',
        'games.urls',
        'games.admin',
        'games.apps',
        'games.templatetags',
        'games.templatetags.game_filters',
        'games.models_parts',
        'games.utils',
        'games.views_parts',
        'psycopg2',
        'psycopg2._psycopg',
        'psycopg2._json',
        'dj_database_url',
        'dotenv',
        'desktop_migrations',
        'desktop_migrations.apps',
        'desktop_migrations.migrations',
        'desktop_migrations.migrations.0001_initial',
        'tqdm',
        'tqdm.std',
        'tqdm._tqdm',
        'tqdm._utils',
        'tqdm._monitor',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'games.migrations',
        'tkinter',
        'IPython',
        'jupyter',
        'notebook',
        'matplotlib',
        'numpy',
        'pandas',
        'scipy',
        'PIL',
        'wx',
        'PyQt5',
        'PySide2',
    ],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='IGDB_Site',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None
)