# engineer.spec
# -*- mode: python ; coding: utf-8 -*-

import os

block_cipher = None

a = Analysis(
    ['ai_engineer.py'],
    pathex=['.'],
    binaries=[],
    datas=[
        # Include sounddevice portaudio DLL if present
    ],
    hiddenimports=[
        'pyttsx3',
        'pyttsx3.drivers',
        'pyttsx3.drivers.sapi5',
        'pyttsx3.drivers.nsss',
        'pyttsx3.drivers.espeak',
        'pynput',
        'pynput.keyboard',
        'pynput.keyboard._win32',
        'pynput.mouse',
        'pynput.mouse._win32',
        'sounddevice',
        'scipy.signal',
        'scipy.io.wavfile',
        'numpy',
        'tkinter',
        'tkinter.ttk',
        'tkinter.scrolledtext',
        'tkinter.filedialog',
        'irsdk',
        'requests',
        'pygame',
        'pygame.joystick',
        'pygame.event',
        'spotter',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['matplotlib', 'PIL', 'cv2', 'pandas'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# Collect all files from packages that need them
from PyInstaller.utils.hooks import collect_all, collect_data_files

for pkg in ['sounddevice', 'scipy']:
    datas_pkg, binaries_pkg, hiddenimports_pkg = collect_all(pkg)
    a.datas    += datas_pkg
    a.binaries += binaries_pkg
    a.hiddenimports += hiddenimports_pkg

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='AIRaceEngineer',
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
    icon='ai_race_engineer.ico' if os.path.exists('ai_race_engineer.ico') else None,
    version='engineer_version.txt',
)
