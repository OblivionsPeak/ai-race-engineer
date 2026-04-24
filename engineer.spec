# engineer.spec
# -*- mode: python ; coding: utf-8 -*-

import os
import sys

block_cipher = None

# Locate the sounddevice portaudio DLL manually (PyInstaller 6.x compatible)
def _find_portaudio():
    try:
        import sounddevice, pathlib
        sd_dir = pathlib.Path(sounddevice.__file__).parent
        dlls = list(sd_dir.glob('*portaudio*')) + list(sd_dir.glob('_sounddevice_data/**/*portaudio*'))
        return [(str(d), '.') for d in dlls if d.is_file()]
    except Exception:
        return []

portaudio_binaries = _find_portaudio()

a = Analysis(
    ['ai_engineer.py'],
    pathex=['.'],
    binaries=portaudio_binaries,
    datas=[('ai_race_engineer.ico', '.')] if os.path.exists('ai_race_engineer.ico') else [],
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
        '_sounddevice_data',
        'scipy',
        'scipy.signal',
        'scipy.io',
        'scipy.io.wavfile',
        'scipy.special._special_ufuncs',
        'scipy.special._cdflib',
        'numpy',
        'numpy.core._multiarray_umath',
        'tkinter',
        'tkinter.ttk',
        'tkinter.scrolledtext',
        'tkinter.filedialog',
        'irsdk',
        'requests',
        'urllib3',
        'certifi',
        'charset_normalizer',
        'pygame',
        'pygame.joystick',
        'pygame.event',
        'spotter',
        'edge_tts',
        'aiohttp',
        'aiofiles',
        'asyncio',
        'asyncio.windows_events',
        'asyncio.windows_utils',
        '_overlapped',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['matplotlib', 'PIL', 'cv2', 'pandas', 'anthropic', 'openai'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='NeuralRacingPerformance',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir='%LOCALAPPDATA%\\AIRaceEngineer',
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='ai_race_engineer.ico' if os.path.exists('ai_race_engineer.ico') else None,
    version='engineer_version.txt',
)
