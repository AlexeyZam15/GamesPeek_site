# -*- mode: python ; coding: utf-8 -*-

import os
import sys
from pathlib import Path

project_root = 'P:/Users/Alexey/Desktop/igdb_site/igdb_site'
settings_path = 'P:/Users/Alexey/Desktop/igdb_site/igdb_site/igdb_site'
venv_site_packages = 'P:/Users/Alexey/Desktop/igdb_site/venv/Lib/site-packages'

sys.path.insert(0, project_root)
sys.path.insert(0, settings_path)
sys.path.insert(0, venv_site_packages)

os.environ['DJANGO_SETTINGS_MODULE'] = 'igdb_site.settings'

datas_list = []

folders_to_copy = [
    ('games/templates', 'games/templates'),
    ('games/static', 'games/static'),
    ('games/templatetags', 'games/templatetags'),
    ('igdb_site', 'igdb_site'),
]

for src, dst in folders_to_copy:
    src_path = os.path.join(project_root, src)
    if os.path.exists(src_path):
        datas_list.append((src_path, dst))

staticfiles_path = Path(project_root) / 'staticfiles'
if staticfiles_path.exists():
    datas_list.append((str(staticfiles_path), 'staticfiles'))

a = Analysis(
    ['loader.py'],
    pathex=[project_root, settings_path, venv_site_packages],
    binaries=[],
    datas=datas_list,
    hiddenimports=[
        'pgembed',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'games.migrations',
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
        'pytest',
        'setuptools',
        'pip',
        'wheel',
        'tkinter.test',
        'unittest',
        'pdb',
        'doctest',
    ],
    noarchive=False,
)

pyz = PYZ(a.pure)

coll = COLLECT(
    EXE(
        pyz,
        a.scripts,
        a.binaries,
        a.datas,
        [],
        name='gamespeek',
        debug=False,
        bootloader_ignore_signals=False,
        strip=False,
        upx=True,
        upx_exclude=[],
        runtime_tmpdir=None,
        console=False,
        disable_windowed_traceback=False,
        argv_emulation=False,
        target_arch=None,
        codesign_identity=None,
        entitlements_file=None,
        icon=None
    ),
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='gamespeek'
)