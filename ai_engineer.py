"""
Endurance Race Planner — AI Race Engineer (Standalone)
=======================================================
Push-to-talk voice assistant + proactive alerts via backend proxy.
Reads iRacing telemetry directly via pyirsdk. No direct Anthropic/OpenAI
API keys required — all AI queries and transcription go through the backend.

Python 3.8+ is the only requirement — the script installs everything else itself.
"""

import json
import math
import os
import subprocess
import sys

# ── Backend server URL — update this before building the public EXE ──────────
BACKEND_URL = "https://endurance-planner-production.up.railway.app"
# ─────────────────────────────────────────────────────────────────────────────

VERSION     = "1.0.4"
GITHUB_REPO = "OblivionsPeak/ai-race-engineer"

# ── Auto-install missing packages before anything else ──────────────────────
def _ensure(package, import_name=None):
    import_name = import_name or package
    try:
        __import__(import_name)
    except ImportError:
        print(f'Installing {package}…')
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', package, '-q'])

_ensure('requests')
_ensure('pyirsdk', 'irsdk')
_ensure('pyttsx3')
_ensure('sounddevice')
_ensure('numpy')
_ensure('scipy')
_ensure('pynput')
_ensure('pygame')
# Note: anthropic and openai are no longer needed client-side
# ── Now safe to import ───────────────────────────────────────────────────────

import requests
import tempfile
import threading
import time
import tkinter as tk
from tkinter import ttk, scrolledtext

import numpy as np

try:
    import irsdk
    IRSDK_AVAILABLE = True
except ImportError:
    IRSDK_AVAILABLE = False

try:
    import pyttsx3
    TTS_AVAILABLE = True
except ImportError:
    TTS_AVAILABLE = False

try:
    import sounddevice as sd
    SD_AVAILABLE = True
except ImportError:
    SD_AVAILABLE = False

try:
    from scipy.io import wavfile
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    from pynput import keyboard as pynput_keyboard
    PYNPUT_AVAILABLE = True
except ImportError:
    PYNPUT_AVAILABLE = False

try:
    import pygame
    PYGAME_AVAILABLE = True
except ImportError:
    PYGAME_AVAILABLE = False


# ---------------------------------------------------------------------------
# Colors  (identical to telemetry_bridge.py)
# ---------------------------------------------------------------------------
BG     = '#07101f'
BG2    = '#0c1830'
BG3    = '#122040'
BORDER = '#1a2f52'
ACCENT = '#c8192e'
GREEN  = '#3ecf8e'
YELLOW = '#f5c542'
TEXT   = '#edf1ff'
DIM    = '#6e85b0'


# ---------------------------------------------------------------------------
# Config persistence  (AppData)
# ---------------------------------------------------------------------------
APPDATA_DIR = os.path.join(os.environ.get('APPDATA', os.path.expanduser('~')), 'AIRaceEngineer')
CONFIG_PATH = os.path.join(APPDATA_DIR, 'config.json')
PLAN_PATH   = os.path.join(APPDATA_DIR, 'race_plan.json')

DEFAULTS = {
    'token':             '',
    'display_name':      '',
    'fuel_warning_laps': 3,
    'fuel_unit':         'gal',
    'ptt_binding':       {'type': 'keyboard', 'key': 'space'},
    'spotter_enabled':   True,
}

# Create AppData directory on startup if it doesn't exist
os.makedirs(APPDATA_DIR, exist_ok=True)


def _binding_label(binding: dict) -> str:
    """Return a human-readable label for a PTT binding dict."""
    if not binding:
        return 'SPACE'
    if binding.get('type') == 'joystick':
        return f'JOY{binding.get("device", 0)} BTN{binding.get("button", 0)}'
    key = binding.get('key', 'space')
    return key.upper()


DEFAULT_RACE_PLAN = {
    'name':              'My Race',
    'race_duration_hrs': 2.5,
    'fuel_capacity_l':   18.5,
    'fuel_per_lap_l':    0.92,
    'lap_time_s':        92.0,
    'pit_loss_s':        35.0,
    'drivers': [
        {'name': 'Driver 1', 'max_hours': 2.5},
    ],
}


def load_config() -> dict:
    try:
        with open(CONFIG_PATH) as f:
            c = json.load(f)
            return {**DEFAULTS, **c}
    except Exception:
        return dict(DEFAULTS)


def save_config(cfg: dict):
    try:
        with open(CONFIG_PATH, 'w') as f:
            json.dump(cfg, f, indent=2)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Strategy calculation
# ---------------------------------------------------------------------------
FUEL_MODES = {'normal': 1.0, 'push': 1.08, 'save': 0.92}


def _calculate_stints(plan: dict) -> list:
    race_s     = plan['race_duration_hrs'] * 3600
    lap_s      = plan['lap_time_s']
    capacity   = plan['fuel_capacity_l']
    base_fpl   = plan['fuel_per_lap_l']
    drivers    = plan.get('drivers', [{'name': 'Driver', 'max_hours': 99}])
    max_hrs    = plan.get('max_continuous_hrs', 2.5)

    fpl            = base_fpl  # normal mode
    laps_per_tank  = int(math.floor((capacity - fpl) / fpl)) if fpl > 0 else 999
    fatigue_laps   = int(math.floor(max_hrs * 3600 / lap_s)) if lap_s > 0 else 999
    laps_per_stint = max(min(laps_per_tank, fatigue_laps), 1)

    stints, current_lap, stint_num, driver_idx, elapsed_s = [], 1, 1, 0, 0.0
    n = max(len(drivers), 1)

    while True:
        driver          = drivers[driver_idx % n]
        remaining_laps  = int(math.floor((race_s - elapsed_s) / lap_s))
        if remaining_laps <= 0:
            break
        stint_laps = min(laps_per_stint, remaining_laps)
        end_lap    = current_lap + stint_laps - 1
        is_last    = remaining_laps <= stint_laps
        stints.append({
            'stint_num':     stint_num,
            'driver_name':   driver.get('name', f'Driver {driver_idx + 1}'),
            'start_lap':     current_lap,
            'end_lap':       end_lap,
            'pit_lap':       end_lap if not is_last else None,
            'fuel_load':     min(round(stint_laps * fpl + fpl, 2), capacity),
            'laps_in_stint': stint_laps,
            'is_last':       is_last,
        })
        elapsed_s  += stint_laps * lap_s
        current_lap = end_lap + 1
        stint_num  += 1
        driver_idx += 1

    return stints


# ---------------------------------------------------------------------------
# Live status calculation
# ---------------------------------------------------------------------------
def _calc_live_status(current_lap: int, stints: list, plan: dict) -> dict:
    fpl      = plan['fuel_per_lap_l']
    lap_s    = plan['lap_time_s']
    capacity = plan['fuel_capacity_l']

    current_stint = next(
        (s for s in stints if s['start_lap'] <= current_lap <= s['end_lap']), None
    )
    if not current_stint:
        return {'status': 'finished', 'current_lap': current_lap}

    next_idx   = stints.index(current_stint) + 1
    next_stint = stints[next_idx] if next_idx < len(stints) else None

    laps_into_stint = max(current_lap - current_stint['start_lap'], 0)
    fuel_remaining  = max(current_stint['fuel_load'] - laps_into_stint * fpl, 0)
    laps_of_fuel    = fuel_remaining / fpl if fpl > 0 else 0
    fuel_pct        = round((fuel_remaining / capacity) * 100) if capacity > 0 else 0
    planned_pit     = current_stint.get('pit_lap')
    laps_until_pit  = (planned_pit or current_stint['end_lap']) - current_lap
    last_safe       = current_lap + max(int(math.floor(laps_of_fuel)) - 1, 0)

    pit_status = 'green'
    if planned_pit:
        if current_lap > planned_pit:
            pit_status = 'red'
        elif laps_until_pit <= 2:
            pit_status = 'yellow'

    return {
        'status':             'racing',
        'current_lap':        current_lap,
        'current_stint':      current_stint,
        'next_stint':         next_stint,
        'laps_until_pit':     laps_until_pit,
        'mins_until_pit':     round(laps_until_pit * lap_s / 60, 1),
        'fuel_remaining_l':   round(fuel_remaining, 1),
        'laps_of_fuel':       round(laps_of_fuel, 1),
        'fuel_pct':           fuel_pct,
        'pit_window_optimal': planned_pit,
        'pit_window_last':    last_safe,
        'pit_window_status':  pit_status,
        'alert':              0 < laps_until_pit <= 3,
    }


# ---------------------------------------------------------------------------
# Auto race plan detection from iRacing session data
# ---------------------------------------------------------------------------
def _build_auto_plan_from_ir(ir) -> dict | None:
    """Read iRacing session variables and return a plan dict, or None on failure."""
    try:
        driver_info  = ir['DriverInfo']  or {}
        weekend_info = ir['WeekendInfo'] or {}
        session_info = ir['SessionInfo'] or {}

        capacity_l   = float(driver_info.get('DriverCarFuelMaxLtr', 0) or 0)
        est_lap_s    = float(driver_info.get('DriverCarEstLapTime', 0) or 0)
        driver_name  = driver_info.get('DriverUserName', 'Driver') or 'Driver'
        track_name   = weekend_info.get('TrackDisplayName', 'Unknown Track') or 'Unknown Track'
        car_name     = weekend_info.get('CarName', '') or weekend_info.get('SeriesName', '')

        if capacity_l <= 0 or est_lap_s <= 0:
            return None

        # Find the Race session to determine duration
        race_duration_hrs = 1.0  # fallback
        sessions = session_info.get('Sessions', []) or []
        for session in sessions:
            if session.get('SessionType', '') == 'Race':
                time_str = str(session.get('SessionTime', '') or '')
                laps_str = str(session.get('SessionLaps', '') or '')
                if 'sec' in time_str and 'unlimited' not in time_str:
                    secs = float(time_str.replace('sec', '').strip())
                    if secs > 0:
                        race_duration_hrs = round(secs / 3600, 3)
                        break
                elif laps_str.isdigit() and est_lap_s > 0:
                    race_duration_hrs = round(int(laps_str) * est_lap_s / 3600, 3)
                    break

        # Estimate fuel-per-lap as 5% of tank (placeholder; refined via rolling telemetry)
        est_fpl = round(capacity_l * 0.05, 2)

        name_parts = [track_name]
        if car_name:
            name_parts.append(car_name)
        plan_name = ' · '.join(name_parts)

        return {
            'name':              plan_name,
            'race_duration_hrs': race_duration_hrs,
            'lap_time_s':        round(est_lap_s, 3),
            'fuel_capacity_l':   round(capacity_l, 2),
            'fuel_per_lap_l':    est_fpl,
            'pit_loss_s':        35.0,
            'auto_detected':     True,
            'drivers':           [{'name': driver_name, 'max_hours': min(race_duration_hrs, 2.5)}],
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# iRacing telemetry thread
# ---------------------------------------------------------------------------
class TelemetryThread(threading.Thread):
    def __init__(self, app_ref):
        super().__init__(daemon=True)
        self._app  = app_ref
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def run(self):
        if not IRSDK_AVAILABLE:
            self._app.log('ERROR: pyirsdk not installed — cannot read telemetry.')
            self._app.set_status('error')
            return

        ir                 = irsdk.IRSDK()
        ir.startup()
        last_fpl_lap       = 0
        prev_fuel          = None
        fuel_history       = []
        auto_plan_detected = False

        while not self._stop.is_set():
            try:
                if not ir.is_initialized or not ir.is_connected:
                    self._app.set_status('connecting')
                    auto_plan_detected = False
                    ir.startup()
                    self._stop.wait(2)
                    continue

                ir.freeze_var_buffer_latest()

                # Auto-detect race plan once per connection if no manual plan exists
                if not auto_plan_detected:
                    auto_plan = _build_auto_plan_from_ir(ir)
                    if auto_plan:
                        auto_plan_detected = True
                        self._app.after(0, lambda p=auto_plan: self._app._apply_auto_plan(p))

                plan      = self._app._plan
                stints    = self._app._stints
                fuel_unit = self._app._cfg.get('fuel_unit', 'gal')

                current_lap   = ir['Lap']             or 0
                fuel_raw      = ir['FuelLevel']        or 0.0
                session_time  = ir['SessionTime']      or 0.0
                lap_last      = ir['LapLastLapTime']   or 0.0
                lap_completed = ir['LapCompleted']     or 0

                # Convert fuel to litres if the plan uses litres
                fuel = fuel_raw * 3.78541 if fuel_unit == 'l' else fuel_raw

                # Rolling fuel-per-lap delta
                fuel_delta = {}
                if lap_completed > last_fpl_lap and prev_fuel is not None:
                    actual_fpl = round(prev_fuel - fuel, 4)
                    if 0.05 < actual_fpl < 5.0:
                        fuel_history.append(actual_fpl)
                        fuel_history = fuel_history[-10:]
                        avg_fpl = round(sum(fuel_history) / len(fuel_history), 4)
                        fuel_delta = {
                            'avg_actual_fpl':  avg_fpl,
                            'last_actual_fpl': actual_fpl,
                            'history':         list(fuel_history),
                        }
                        # Update auto-detected plan's FPL once we have 3+ laps of data
                        if len(fuel_history) >= 3 and self._app._plan.get('auto_detected'):
                            self._app.after(0, lambda f=avg_fpl: self._app._update_auto_fpl(f))
                    last_fpl_lap = lap_completed
                prev_fuel = fuel

                live = _calc_live_status(current_lap, stints, plan) if stints else {}

                ctx = {
                    'plan': {
                        **plan,
                        'stints':            stints,
                        'total_stints':      len(stints),
                        'pit_stops_planned': max(len(stints) - 1, 0),
                    },
                    'live': live,
                    'telemetry': {
                        'current_lap':     current_lap,
                        'fuel_level':      round(fuel, 3),
                        'last_lap_time_s': round(lap_last, 3) if lap_last > 0 else None,
                        'session_time_s':  round(session_time, 1),
                        'fuel_delta':      fuel_delta,
                        'stale':           False,
                    },
                }

                with self._app._ctx_lock:
                    self._app._ctx = ctx
                self._app.set_status('connected')
                self._app.after(0, self._app._refresh_stint_panel)

            except Exception as e:
                self._app.log(f'Telemetry error: {e}')

            self._stop.wait(1.0)

        self._app.log('Telemetry thread stopped.')


# ---------------------------------------------------------------------------
# Auto-updater
# ---------------------------------------------------------------------------
def _parse_version(tag: str) -> tuple:
    """Convert 'v1.2.3' or '1.2.3' to (1, 2, 3) for comparison."""
    return tuple(int(x) for x in tag.lstrip('v').split('.') if x.isdigit())


def _check_update_available() -> tuple[str, str] | None:
    """
    Query GitHub Releases API. Returns (tag, download_url) if a newer version
    exists, or None if up-to-date or the check fails.
    """
    try:
        r = requests.get(
            f'https://api.github.com/repos/{GITHUB_REPO}/releases/latest',
            headers={'Accept': 'application/vnd.github+json'},
            timeout=8,
        )
        if not r.ok:
            return None
        data     = r.json()
        tag      = data.get('tag_name', '')
        assets   = data.get('assets', [])
        exe_url  = next(
            (a['browser_download_url'] for a in assets
             if a['name'].endswith('.exe')),
            None,
        )
        if not tag or not exe_url:
            return None
        if _parse_version(tag) > _parse_version(VERSION):
            return tag, exe_url
    except Exception:
        pass
    return None


def _apply_update(new_exe_path: str):
    """
    Replace the running EXE with new_exe_path using a .bat trampoline,
    then exit. Works around Windows locking the running EXE.
    """
    current_exe = sys.executable if getattr(sys, 'frozen', False) else None
    if not current_exe:
        return  # running as a .py script — skip

    bat_path = os.path.join(tempfile.gettempdir(), '_aire_update.bat')
    bat = (
        '@echo off\n'
        'timeout /t 2 /nobreak >nul\n'
        f'move /y "{new_exe_path}" "{current_exe}"\n'
        f'start "" "{current_exe}"\n'
        'del "%~f0"\n'
    )
    with open(bat_path, 'w') as f:
        f.write(bat)
    subprocess.Popen(
        ['cmd', '/c', bat_path],
        creationflags=subprocess.CREATE_NO_WINDOW,
        close_fds=True,
    )
    sys.exit(0)


# ---------------------------------------------------------------------------
# Main App
# ---------------------------------------------------------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('Endurance Race Planner — AI Race Engineer')
        self.configure(bg=BG)
        self.resizable(True, True)
        self.minsize(560, 720)

        cfg = load_config()

        # State
        self._ctx:  dict | None = None
        self._plan: dict        = {}
        self._stints: list      = []
        self._cfg:  dict        = cfg
        self._ctx_lock           = threading.Lock()
        self._stop_evt           = threading.Event()
        self._running            = False
        self._recording          = False
        self._audio_chunks: list = []
        self._kb_listener        = None
        self._telemetry_thread: TelemetryThread | None = None
        self._spotter_thread     = None
        self._last_fuel_alert    = 0.0
        self._ptt_down           = False
        self._last_pit_alert     = 0.0
        self._last_overdue_alert = 0.0
        self._joystick_thread: threading.Thread | None = None
        self._queries_today      = 0
        self._query_limit        = 50
        self._display_name       = self._cfg.get('display_name', '')

        # ── Style ────────────────────────────────────────────────────────
        style = ttk.Style(self)
        style.theme_use('clam')
        style.configure('TLabel',      background=BG,  foreground=TEXT, font=('Segoe UI', 9))
        style.configure('TFrame',      background=BG)
        style.configure('TLabelframe', background=BG2, foreground=DIM, relief='flat')
        style.configure('TLabelframe.Label', background=BG2, foreground=DIM,
                        font=('Segoe UI', 8, 'bold'))
        style.configure('TEntry',      fieldbackground=BG3, foreground=TEXT,
                        insertcolor=TEXT, bordercolor=BORDER, relief='flat')
        style.configure('TCombobox',   fieldbackground=BG3, foreground=TEXT,
                        selectbackground=BG3)
        style.map('TEntry',    bordercolor=[('focus', ACCENT)])
        style.map('TCombobox', fieldbackground=[('readonly', BG3)])
        style.configure('Start.TButton', background=ACCENT, foreground='white',
                        font=('Segoe UI', 10, 'bold'), relief='flat', padding=(16, 8))
        style.map('Start.TButton', background=[('active', '#a01020')])
        style.configure('Stop.TButton', background=BG3, foreground=YELLOW,
                        font=('Segoe UI', 10, 'bold'), relief='flat', padding=(16, 8))
        style.map('Stop.TButton', background=[('active', '#1a2f52')])
        style.configure('Ask.TButton', background=BG3, foreground=GREEN,
                        font=('Segoe UI', 9, 'bold'), relief='flat', padding=(10, 6))
        style.map('Ask.TButton', background=[('active', '#1a2f52')])
        style.configure('Browse.TButton', background=BG3, foreground=TEXT,
                        font=('Segoe UI', 9), relief='flat', padding=(6, 4))
        style.map('Browse.TButton', background=[('active', '#1a2f52')])
        style.configure('Wizard.TButton', background=ACCENT, foreground='white',
                        font=('Segoe UI', 10, 'bold'), relief='flat', padding=(12, 7))
        style.map('Wizard.TButton', background=[('active', '#a01020')])
        style.configure('WizardSec.TButton', background=BG3, foreground=TEXT,
                        font=('Segoe UI', 10), relief='flat', padding=(12, 7))
        style.map('WizardSec.TButton', background=[('active', '#1a2f52')])

        # ── String vars for new settings rows ────────────────────────────
        self.v_spotter    = tk.BooleanVar(value=self._cfg.get('spotter_enabled', True))
        self.v_fuel_unit  = tk.StringVar(value=self._cfg.get('fuel_unit', 'gal'))
        self.v_ptt_label  = tk.StringVar(
            value=_binding_label(self._cfg.get('ptt_binding', DEFAULTS['ptt_binding'])))
        self.v_acct_label = tk.StringVar(value=self._display_name or '(not logged in)')
        self.v_queries    = tk.StringVar(value='— / — queries today')
        self.v_plan_name  = tk.StringVar(value='No plan loaded')

        # ── Build UI ─────────────────────────────────────────────────────
        self._build_header()
        self._build_config(cfg)
        self._build_status_and_buttons()
        self._build_stint_panel()
        self._build_voice_section()
        self._build_qa_display()
        self._build_log()

        self.protocol('WM_DELETE_WINDOW', self.on_close)

        # ── Auth check on boot ────────────────────────────────────────────
        self.after(100, self._check_auth_on_boot)

        # ── Update check (background, 3 s delay so UI is ready first) ────
        if getattr(sys, 'frozen', False):
            self.after(3000, lambda: threading.Thread(
                target=self._check_for_update, daemon=True).start())

    # ── UI builders ──────────────────────────────────────────────────────────

    def _build_header(self):
        hdr = ttk.Frame(self)
        hdr.pack(fill='x', pady=(14, 8), padx=14)
        tk.Label(hdr, text='⬡', bg=BG, fg=ACCENT, font=('Segoe UI', 18)).pack(side='left')
        tk.Label(hdr, text='  AI RACE ENGINEER', bg=BG, fg=TEXT,
                 font=('Segoe UI', 12, 'bold')).pack(side='left')
        tk.Label(hdr, text='OpMo eSports', bg=BG, fg=DIM,
                 font=('Segoe UI', 9)).pack(side='left', padx=(8, 0))

    def _build_config(self, cfg: dict):
        frm = ttk.LabelFrame(self, text='SETTINGS', padding=10)
        frm.pack(fill='x', padx=14, pady=4)
        frm.columnconfigure(1, weight=1)

        # Row 0: Account
        ttk.Label(frm, text='Account').grid(row=0, column=0, sticky='w', pady=3, padx=(0, 10))
        tk.Label(frm, textvariable=self.v_acct_label, bg=BG3, fg=GREEN,
                 font=('Segoe UI', 9), padx=8, pady=3).grid(
                     row=0, column=1, sticky='ew', pady=3)
        ttk.Button(frm, text='Log Out', style='Browse.TButton',
                   command=self._logout).grid(row=0, column=2, sticky='w', padx=(4, 0), pady=3)

        # Row 1: Queries
        ttk.Label(frm, text='Queries').grid(row=1, column=0, sticky='w', pady=3, padx=(0, 10))
        tk.Label(frm, textvariable=self.v_queries, bg=BG2, fg=DIM,
                 font=('Segoe UI', 9), padx=8, pady=3).grid(
                     row=1, column=1, sticky='ew', pady=3, columnspan=2)

        # Row 2: Race Plan
        ttk.Label(frm, text='Race Plan').grid(row=2, column=0, sticky='w', pady=3, padx=(0, 10))
        tk.Label(frm, textvariable=self.v_plan_name, bg=BG3, fg=TEXT,
                 font=('Segoe UI', 9), padx=8, pady=3).grid(
                     row=2, column=1, sticky='ew', pady=3)
        ttk.Button(frm, text='Edit Plan', style='Browse.TButton',
                   command=self._show_plan_editor).grid(row=2, column=2, sticky='w', padx=(4, 0), pady=3)

        # Row 3: PTT Button
        ttk.Label(frm, text='PTT Button').grid(row=3, column=0, sticky='w', pady=3, padx=(0, 10))
        tk.Label(frm, textvariable=self.v_ptt_label, bg=BG3, fg=YELLOW,
                 font=('Consolas', 9, 'bold'), padx=8, pady=3).grid(
                     row=3, column=1, sticky='w', pady=3)
        ttk.Button(frm, text='Change…', style='Browse.TButton',
                   command=self._rebind_ptt).grid(row=3, column=2, sticky='w', padx=(4, 0), pady=3)

        # Row 4: Fuel Unit
        ttk.Label(frm, text='Fuel Unit').grid(row=4, column=0, sticky='w', pady=3, padx=(0, 10))
        cb = ttk.Combobox(frm, textvariable=self.v_fuel_unit, values=['gal', 'l'],
                          state='readonly', width=6)
        cb.grid(row=4, column=1, sticky='w', pady=3)

        # Row 5: Spotter
        ttk.Label(frm, text='Spotter').grid(row=5, column=0, sticky='w', pady=3, padx=(0, 10))
        tk.Checkbutton(
            frm, text='Enable spotter callouts',
            variable=self.v_spotter,
            bg=BG2, fg=TEXT, selectcolor=BG3,
            activebackground=BG2, activeforeground=TEXT,
            font=('Segoe UI', 9),
            command=self._save_spotter_pref,
        ).grid(row=5, column=1, sticky='w', pady=3, columnspan=2)

    def _save_spotter_pref(self):
        self._cfg['spotter_enabled'] = self.v_spotter.get()
        save_config(self._cfg)

    def _check_for_update(self):
        """Run in a background thread. Prompts user if a newer release exists."""
        result = _check_update_available()
        if not result:
            return
        tag, exe_url = result
        self.after(0, lambda: self._prompt_update(tag, exe_url))

    def _prompt_update(self, tag: str, exe_url: str):
        """Show update dialog on the main thread."""
        dlg = tk.Toplevel(self)
        dlg.title('Update Available')
        dlg.configure(bg=BG)
        dlg.resizable(False, False)
        dlg.grab_set()
        dlg.geometry('380x200')

        tk.Label(dlg, text='Update Available', bg=BG, fg=TEXT,
                 font=('Segoe UI', 13, 'bold')).pack(pady=(22, 4))
        tk.Label(dlg, text=f'Version {tag} is ready.  You have v{VERSION}.',
                 bg=BG, fg=DIM, font=('Segoe UI', 9)).pack()
        tk.Label(dlg, text='The app will restart automatically after downloading.',
                 bg=BG, fg=DIM, font=('Segoe UI', 9)).pack(pady=(2, 14))

        progress_var = tk.DoubleVar(value=0)
        bar = ttk.Progressbar(dlg, variable=progress_var, maximum=100, length=300)
        bar.pack(pady=(0, 8))

        status_var = tk.StringVar(value='')
        tk.Label(dlg, textvariable=status_var, bg=BG, fg=YELLOW,
                 font=('Segoe UI', 9)).pack()

        btn_frame = tk.Frame(dlg, bg=BG)
        btn_frame.pack(pady=10)
        install_btn = ttk.Button(btn_frame, text='Install Now', style='Wizard.TButton')
        install_btn.pack(side='left', padx=(0, 8))
        ttk.Button(btn_frame, text='Not Now', style='WizardSec.TButton',
                   command=dlg.destroy).pack(side='left')

        def do_install():
            install_btn.config(state='disabled')
            status_var.set('Downloading…')

            def download():
                try:
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.exe')
                    tmp.close()
                    with requests.get(exe_url, stream=True, timeout=120) as resp:
                        resp.raise_for_status()
                        total = int(resp.headers.get('content-length', 0))
                        done  = 0
                        with open(tmp.name, 'wb') as f:
                            for chunk in resp.iter_content(chunk_size=65536):
                                if chunk:
                                    f.write(chunk)
                                    done += len(chunk)
                                    if total:
                                        pct = done / total * 100
                                        self.after(0, lambda p=pct: progress_var.set(p))
                    self.after(0, lambda: status_var.set('Installing…'))
                    self.after(500, lambda: _apply_update(tmp.name))
                except Exception as e:
                    self.after(0, lambda err=e: (
                        status_var.set(f'Download failed: {err}'),
                        install_btn.config(state='normal'),
                    ))

            threading.Thread(target=download, daemon=True).start()

        install_btn.config(command=do_install)

    def _logout(self):
        self._cfg['token']        = ''
        self._cfg['display_name'] = ''
        save_config(self._cfg)
        self._display_name = ''
        self.v_acct_label.set('(not logged in)')
        self.v_queries.set('— / — queries today')
        self.log('Logged out.')
        self._show_wizard()

    def _update_plan_display(self):
        plan = self._plan
        if not plan:
            self.v_plan_name.set('No plan loaded')
            return
        name = plan.get('name', 'Unnamed Plan')
        if plan.get('auto_detected'):
            self.v_plan_name.set(f'Auto: {name}')
        else:
            self.v_plan_name.set(name)

    def _apply_auto_plan(self, plan: dict):
        """Apply an auto-detected plan from iRacing — only if no manual plan is loaded."""
        if self._plan and not self._plan.get('auto_detected'):
            return  # manual plan takes priority
        try:
            stints       = _calculate_stints(plan)
            self._plan   = plan
            self._stints = stints
            self._update_plan_display()
            self.log(
                f'[AUTO] Race detected: {plan["name"]}  '
                f'{plan["race_duration_hrs"]}h  '
                f'Tank: {plan["fuel_capacity_l"]}L  '
                f'Est.FPL: {plan["fuel_per_lap_l"]}L (refining…)'
            )
        except Exception as e:
            self.log(f'[AUTO] Plan apply error: {e}')

    def _update_auto_fpl(self, avg_fpl: float):
        """Refine the auto-detected fuel-per-lap from rolling telemetry data."""
        if not self._plan or not self._plan.get('auto_detected'):
            return
        if abs(self._plan.get('fuel_per_lap_l', 0) - avg_fpl) < 0.001:
            return  # no meaningful change
        self._plan['fuel_per_lap_l'] = avg_fpl
        try:
            self._stints = _calculate_stints(self._plan)
            self._update_plan_display()
            self.log(f'[AUTO] Fuel/lap refined to {avg_fpl}L from telemetry.')
        except Exception:
            pass

    def _rebind_ptt(self):
        """Open a modal dialog that captures the next key or joystick button press."""
        dlg = tk.Toplevel(self)
        dlg.title('Set PTT Button')
        dlg.configure(bg=BG)
        dlg.resizable(False, False)
        dlg.grab_set()
        dlg.geometry('320x160')

        tk.Label(dlg, text='Press any key or steering wheel button',
                 bg=BG, fg=TEXT, font=('Segoe UI', 11, 'bold')).pack(pady=(20, 4), padx=20)
        tk.Label(dlg, text='Hold it briefly then release',
                 bg=BG, fg=DIM, font=('Segoe UI', 9)).pack()
        status_var = tk.StringVar(value='Listening…')
        tk.Label(dlg, textvariable=status_var, bg=BG, fg=YELLOW,
                 font=('Consolas', 12, 'bold')).pack(pady=10)
        ttk.Button(dlg, text='Cancel', command=dlg.destroy).pack()

        detected    = {'done': False}
        kb_listener = [None]

        def finish(binding: dict, label: str):
            if detected['done']:
                return
            detected['done'] = True
            self._cfg['ptt_binding'] = binding
            save_config(self._cfg)
            self.v_ptt_label.set(label)
            self.after(0, lambda: status_var.set(f'Bound: {label}'))
            self.after(800, dlg.destroy)

        # Keyboard listener
        if PYNPUT_AVAILABLE:
            def on_key_press(key):
                if detected['done']:
                    return False
                try:
                    key_name = key.name
                except AttributeError:
                    key_name = key.char or ''
                if key_name:
                    finish({'type': 'keyboard', 'key': key_name}, key_name.upper())
                return False

            kb_listener[0] = pynput_keyboard.Listener(on_press=on_key_press)
            kb_listener[0].start()

        # Joystick polling thread
        def joy_poll():
            if not PYGAME_AVAILABLE:
                self.after(0, lambda: status_var.set('pygame not available — keyboard only'))
                return
            try:
                if not pygame.get_init():
                    pygame.init()
                pygame.joystick.init()
                count = pygame.joystick.get_count()
                if count == 0:
                    self.after(0, lambda: status_var.set('No joystick found — press a keyboard key'))
                    return
                joysticks = []
                for i in range(count):
                    j = pygame.joystick.Joystick(i)
                    j.init()
                    joysticks.append(j)
                names = ', '.join(j.get_name() for j in joysticks)
                self.after(0, lambda n=names: status_var.set(f'Found: {n}\nPress a wheel button…'))
                # Snapshot button states so we don't fire on already-held buttons
                pygame.event.pump()
                initial = {j.get_id(): [j.get_button(b) for b in range(j.get_numbuttons())]
                           for j in joysticks}
                time.sleep(0.05)
                while not detected['done']:
                    pygame.event.pump()
                    for j in joysticks:
                        jid = j.get_id()
                        for b in range(j.get_numbuttons()):
                            was_down = initial.get(jid, [False]*100)[b] if b < len(initial.get(jid, [])) else False
                            if j.get_button(b) and not was_down and not detected['done']:
                                binding = {'type': 'joystick',
                                           'device_name': j.get_name(),
                                           'device': j.get_id(), 'button': b}
                                label   = f'{j.get_name()} BTN{b}'
                                if kb_listener[0]:
                                    try:
                                        kb_listener[0].stop()
                                    except Exception:
                                        pass
                                finish(binding, label)
                                return
                    time.sleep(0.01)
            except Exception as e:
                self.after(0, lambda err=e: status_var.set(f'Joystick error: {err}'))

        threading.Thread(target=joy_poll, daemon=True).start()

        def on_close():
            detected['done'] = True
            if kb_listener[0]:
                try:
                    kb_listener[0].stop()
                except Exception:
                    pass
            dlg.destroy()

        dlg.protocol('WM_DELETE_WINDOW', on_close)

    def _build_status_and_buttons(self):
        sf = ttk.Frame(self)
        sf.pack(fill='x', padx=14, pady=(6, 2))
        self.status_dot   = tk.Label(sf, text='●', bg=BG, fg=BORDER, font=('Segoe UI', 11))
        self.status_dot.pack(side='left')
        self.status_label = tk.Label(sf, text='Not connected', bg=BG, fg=DIM,
                                     font=('Segoe UI', 9))
        self.status_label.pack(side='left', padx=(4, 0))

        bf = ttk.Frame(self)
        bf.pack(fill='x', padx=14, pady=6)
        self.start_btn = ttk.Button(bf, text='▶  Start Engineer', style='Start.TButton',
                                    command=self.start_engineer)
        self.start_btn.pack(side='left', padx=(0, 8))
        self.stop_btn = ttk.Button(bf, text='■  Stop', style='Stop.TButton',
                                   command=self.stop_engineer, state='disabled')
        self.stop_btn.pack(side='left')
        tk.Label(bf, text='iRacing must be running before starting.',
                 bg=BG, fg=DIM, font=('Segoe UI', 8)).pack(side='right')

    def _build_stint_panel(self):
        pf = ttk.LabelFrame(self, text='LIVE RACE STATE', padding=10)
        pf.pack(fill='x', padx=14, pady=4)

        self._stint_vars = {
            'driver': tk.StringVar(value='—'),
            'lap':    tk.StringVar(value='—'),
            'fuel':   tk.StringVar(value='—'),
            'pit':    tk.StringVar(value='—'),
        }
        labels = [
            ('DRIVER', 'driver', 0, 0),
            ('LAP',    'lap',    0, 2),
            ('FUEL %', 'fuel',   1, 0),
            ('TO PIT', 'pit',    1, 2),
        ]
        for col in (0, 1, 2, 3):
            pf.columnconfigure(col, weight=1)

        for lbl_text, key, row, col in labels:
            tk.Label(pf, text=lbl_text, bg=BG2, fg=DIM,
                     font=('Segoe UI', 7, 'bold')).grid(
                         row=row * 2, column=col, sticky='w', padx=6)
            tk.Label(pf, textvariable=self._stint_vars[key], bg=BG2, fg=TEXT,
                     font=('Segoe UI', 13, 'bold')).grid(
                         row=row * 2 + 1, column=col, sticky='w', padx=6, pady=(0, 6))

        self._waiting_label = tk.Label(
            pf, text='Waiting for iRacing…', bg=BG2, fg=DIM,
            font=('Segoe UI', 9, 'italic'),
        )
        self._waiting_label.grid(row=4, column=0, columnspan=4, pady=(4, 0))

    def _build_voice_section(self):
        vf = ttk.Frame(self)
        vf.pack(fill='x', padx=14, pady=(4, 2))

        binding   = self._cfg.get('ptt_binding', DEFAULTS['ptt_binding'])
        btn_label = _binding_label(binding)
        self.talk_label = tk.Label(
            vf, text=f'HOLD  {btn_label}  TO  TALK',
            bg=BG, fg=DIM,
            font=('Segoe UI', 14, 'bold'),
            pady=10,
        )
        self.talk_label.pack(fill='x')

        # Text fallback (hidden by default)
        self.text_input_frame = ttk.Frame(self)
        self.v_question = tk.StringVar()
        self.question_entry = ttk.Entry(self.text_input_frame, textvariable=self.v_question)
        self.question_entry.pack(side='left', fill='x', expand=True, padx=(0, 6))
        self.question_entry.bind('<Return>', lambda e: self._ask_from_text())
        ttk.Button(self.text_input_frame, text='Ask', style='Ask.TButton',
                   command=self._ask_from_text).pack(side='left')
        self.text_input_frame.pack_forget()

    def _build_qa_display(self):
        qf = ttk.LabelFrame(self, text='LAST Q&A', padding=6)
        qf.pack(fill='x', padx=14, pady=4)
        self.qa_box = scrolledtext.ScrolledText(
            qf, bg=BG3, fg=TEXT, insertbackground=TEXT,
            font=('Segoe UI', 9), relief='flat', bd=0,
            state='disabled', wrap='word', height=6,
        )
        self.qa_box.pack(fill='both', expand=True)

    def _build_log(self):
        lf = ttk.LabelFrame(self, text='LOG', padding=6)
        lf.pack(fill='both', expand=True, padx=14, pady=(4, 14))
        self.log_box = scrolledtext.ScrolledText(
            lf, bg=BG3, fg=TEXT, insertbackground=TEXT,
            font=('Consolas', 8), relief='flat', bd=0,
            state='disabled', wrap='word',
        )
        self.log_box.pack(fill='both', expand=True)

    # ── Auth / boot flow ─────────────────────────────────────────────────────

    def _check_auth_on_boot(self):
        token = self._cfg.get('token', '')
        if not token:
            self._show_wizard()
            return

        # Try a quick token validation ping
        try:
            r = requests.post(
                f'{BACKEND_URL}/engineer/validate',
                json={'token': token},
                timeout=8,
            )
            if r.ok:
                data = self._cfg.get('display_name', '')
                resp = r.json()
                self._display_name = resp.get('display_name', data)
                self._queries_today = resp.get('queries_today', 0)
                self._query_limit   = resp.get('query_limit', 50)
                self._cfg['display_name'] = self._display_name
                save_config(self._cfg)
                self.v_acct_label.set(self._display_name or 'Logged in')
                remaining = self._query_limit - self._queries_today
                self.v_queries.set(
                    f'{self._queries_today} / {self._query_limit} today  '
                    f'({remaining} remaining)'
                )
            else:
                self.log('Session expired — please log in again.')
                self._cfg['token'] = ''
                save_config(self._cfg)
                self._show_wizard()
                return
        except Exception:
            # Offline / server unreachable — proceed with cached token
            self.v_acct_label.set(self._cfg.get('display_name', 'Logged in (offline)'))

        # Load plan if it exists
        if os.path.exists(PLAN_PATH):
            try:
                with open(PLAN_PATH) as f:
                    plan = json.load(f)
                self._plan   = plan
                self._stints = _calculate_stints(plan)
                self._update_plan_display()
            except Exception:
                self._plan   = {}
                self._stints = []
        else:
            # Plan missing — open wizard at plan step
            self._show_wizard(start_at_plan=True)

    # ── First-run wizard ─────────────────────────────────────────────────────

    def _show_wizard(self, start_at_plan: bool = False):
        """Modal wizard: account login/register → race plan setup."""
        dlg = tk.Toplevel(self)
        dlg.title('AI Race Engineer — Setup')
        dlg.configure(bg=BG)
        dlg.resizable(False, False)
        dlg.grab_set()
        dlg.geometry('460x520')

        # Prevent closing mid-wizard if no token exists
        def _on_wizard_close():
            if self._cfg.get('token'):
                dlg.destroy()

        dlg.protocol('WM_DELETE_WINDOW', _on_wizard_close)

        # Container that we swap out between steps
        container = tk.Frame(dlg, bg=BG)
        container.pack(fill='both', expand=True, padx=24, pady=20)

        def clear():
            for w in container.winfo_children():
                w.destroy()

        def label(parent, text, size=10, color=TEXT, bold=False, pady=0):
            font = ('Segoe UI', size, 'bold') if bold else ('Segoe UI', size)
            tk.Label(parent, text=text, bg=BG, fg=color,
                     font=font, wraplength=400).pack(pady=pady)

        def entry_row(parent, lbl_text, show=''):
            tk.Label(parent, text=lbl_text, bg=BG, fg=DIM,
                     font=('Segoe UI', 9), anchor='w').pack(fill='x', pady=(6, 0))
            var = tk.StringVar()
            e   = ttk.Entry(parent, textvariable=var, show=show)
            e.pack(fill='x', pady=(0, 2))
            return var

        def err_label(parent):
            var = tk.StringVar()
            tk.Label(parent, textvariable=var, bg=BG, fg=ACCENT,
                     font=('Segoe UI', 8), wraplength=400).pack(pady=2)
            return var

        # ── Step 1: Welcome ──────────────────────────────────────────────
        def show_welcome():
            clear()
            label(container, 'Welcome to AI Race Engineer', size=14, bold=True, pady=(0, 4))
            label(container, 'By OpMo eSports', size=9, color=DIM, pady=(0, 20))
            label(container, 'Get live strategy advice and spotter callouts\npowered by AI — directly in iRacing.', size=10, color=DIM, pady=(0, 30))
            ttk.Button(container, text='Create Free Account', style='Wizard.TButton',
                       command=show_register).pack(fill='x', pady=(0, 8))
            ttk.Button(container, text='I have an account', style='WizardSec.TButton',
                       command=show_login).pack(fill='x')

        # ── Step 2a: Register ────────────────────────────────────────────
        def show_register():
            clear()
            label(container, 'Create Your Account', size=13, bold=True, pady=(0, 12))
            v_name  = entry_row(container, 'Display Name')
            v_email = entry_row(container, 'Email')
            v_pass  = entry_row(container, 'Password (8+ characters)', show='*')
            v_err   = err_label(container)

            def do_register():
                name  = v_name.get().strip()
                email = v_email.get().strip()
                pw    = v_pass.get()
                if not name:
                    v_err.set('Display name is required.')
                    return
                if not email or '@' not in email:
                    v_err.set('Valid email is required.')
                    return
                if len(pw) < 8:
                    v_err.set('Password must be at least 8 characters.')
                    return
                v_err.set('Creating account…')
                def _req():
                    try:
                        r = requests.post(
                            f'{BACKEND_URL}/engineer/register',
                            json={'display_name': name, 'email': email, 'password': pw},
                            timeout=15,
                        )
                        data = r.json()
                        if r.ok:
                            self._cfg['token']        = data.get('token', '')
                            self._cfg['display_name'] = name
                            save_config(self._cfg)
                            self._display_name = name
                            self.after(0, lambda: (
                                self.v_acct_label.set(name),
                                show_plan_step(),
                            ))
                        else:
                            msg = data.get('error', 'Registration failed.')
                            self.after(0, lambda: v_err.set(msg))
                    except Exception as e:
                        self.after(0, lambda: v_err.set(f'Network error: {e}'))
                threading.Thread(target=_req, daemon=True).start()

            ttk.Button(container, text='Create Account', style='Wizard.TButton',
                       command=do_register).pack(fill='x', pady=(12, 4))
            ttk.Button(container, text='← Back', style='WizardSec.TButton',
                       command=show_welcome).pack(fill='x')

        # ── Step 2b: Login ───────────────────────────────────────────────
        def show_login():
            clear()
            label(container, 'Sign In', size=13, bold=True, pady=(0, 12))
            v_email = entry_row(container, 'Email')
            v_pass  = entry_row(container, 'Password', show='*')
            v_err   = err_label(container)

            def do_login():
                email = v_email.get().strip()
                pw    = v_pass.get()
                if not email:
                    v_err.set('Email is required.')
                    return
                if not pw:
                    v_err.set('Password is required.')
                    return
                v_err.set('Signing in…')
                def _req():
                    try:
                        r = requests.post(
                            f'{BACKEND_URL}/engineer/login',
                            json={'email': email, 'password': pw},
                            timeout=15,
                        )
                        data = r.json()
                        if r.ok:
                            token   = data.get('token', '')
                            dname   = data.get('display_name', email)
                            queries = data.get('queries_today', 0)
                            qlimit  = data.get('query_limit', 50)
                            self._cfg['token']        = token
                            self._cfg['display_name'] = dname
                            save_config(self._cfg)
                            self._display_name  = dname
                            self._queries_today = queries
                            self._query_limit   = qlimit
                            remaining = qlimit - queries
                            self.after(0, lambda: (
                                self.v_acct_label.set(dname),
                                self.v_queries.set(
                                    f'{queries} / {qlimit} today  ({remaining} remaining)'
                                ),
                                show_plan_step(),
                            ))
                        else:
                            msg = data.get('error', 'Login failed.')
                            self.after(0, lambda: v_err.set(msg))
                    except Exception as e:
                        self.after(0, lambda: v_err.set(f'Network error: {e}'))
                threading.Thread(target=_req, daemon=True).start()

            ttk.Button(container, text='Sign In', style='Wizard.TButton',
                       command=do_login).pack(fill='x', pady=(12, 4))
            ttk.Button(container, text='← Back', style='WizardSec.TButton',
                       command=show_welcome).pack(fill='x')

        # ── Step 3: Race Plan Setup ──────────────────────────────────────
        def show_plan_step():
            # If plan already exists, skip straight to done
            if os.path.exists(PLAN_PATH) and not start_at_plan:
                try:
                    with open(PLAN_PATH) as f:
                        plan = json.load(f)
                    self._plan   = plan
                    self._stints = _calculate_stints(plan)
                    self._update_plan_display()
                    dlg.destroy()
                    return
                except Exception:
                    pass

            clear()
            label(container, 'Set Up Your Race Plan', size=13, bold=True, pady=(0, 8))

            # Scrollable frame
            canvas     = tk.Canvas(container, bg=BG, highlightthickness=0, height=300)
            scrollbar  = ttk.Scrollbar(container, orient='vertical', command=canvas.yview)
            scroll_frm = tk.Frame(canvas, bg=BG)
            scroll_frm.bind('<Configure>', lambda e: canvas.configure(
                scrollregion=canvas.bbox('all')))
            canvas.create_window((0, 0), window=scroll_frm, anchor='nw')
            canvas.configure(yscrollcommand=scrollbar.set)
            canvas.pack(side='left', fill='both', expand=True)
            scrollbar.pack(side='right', fill='y')

            def field(lbl, default):
                tk.Label(scroll_frm, text=lbl, bg=BG, fg=DIM,
                         font=('Segoe UI', 9), anchor='w').pack(fill='x', pady=(4, 0))
                var = tk.StringVar(value=str(default))
                ttk.Entry(scroll_frm, textvariable=var).pack(fill='x')
                return var

            v_race_name  = field('Race Name', 'My Race')
            v_duration   = field('Duration (hours)', '2.5')
            v_lap_time   = field('Lap Time Target (seconds)', '92.0')
            v_capacity   = field('Fuel Capacity (litres)', '18.5')
            v_fpl        = field('Fuel Per Lap (litres)', '0.92')
            v_pit_loss   = field('Pit Loss Time (seconds)', '35.0')

            tk.Label(scroll_frm, text='Drivers', bg=BG, fg=DIM,
                     font=('Segoe UI', 9, 'bold'), anchor='w').pack(fill='x', pady=(10, 2))

            drivers_frame = tk.Frame(scroll_frm, bg=BG)
            drivers_frame.pack(fill='x')
            driver_rows = []  # list of (name_var, hours_var, row_frame)

            def add_driver_row(name='Driver', hours='2.5'):
                rf = tk.Frame(drivers_frame, bg=BG)
                rf.pack(fill='x', pady=2)
                vn = tk.StringVar(value=name)
                vh = tk.StringVar(value=str(hours))
                ttk.Entry(rf, textvariable=vn, width=16).pack(side='left', padx=(0, 4))
                tk.Label(rf, text='Max hrs:', bg=BG, fg=DIM,
                         font=('Segoe UI', 8)).pack(side='left')
                ttk.Entry(rf, textvariable=vh, width=6).pack(side='left', padx=(2, 4))

                def remove():
                    driver_rows.remove((vn, vh, rf))
                    rf.destroy()

                tk.Button(rf, text='✕', bg=BG3, fg=ACCENT,
                          font=('Segoe UI', 8), relief='flat',
                          command=remove).pack(side='left')
                driver_rows.append((vn, vh, rf))

            add_driver_row()
            ttk.Button(scroll_frm, text='+ Add Driver', style='Browse.TButton',
                       command=lambda: add_driver_row()).pack(anchor='w', pady=4)

            v_err = tk.StringVar()
            tk.Label(container, textvariable=v_err, bg=BG, fg=ACCENT,
                     font=('Segoe UI', 8), wraplength=400).pack(pady=2)

            def finish_plan():
                try:
                    plan = {
                        'name':              v_race_name.get().strip() or 'My Race',
                        'race_duration_hrs': float(v_duration.get()),
                        'lap_time_s':        float(v_lap_time.get()),
                        'fuel_capacity_l':   float(v_capacity.get()),
                        'fuel_per_lap_l':    float(v_fpl.get()),
                        'pit_loss_s':        float(v_pit_loss.get()),
                        'drivers': [
                            {'name': vn.get().strip() or f'Driver {i+1}',
                             'max_hours': float(vh.get())}
                            for i, (vn, vh, _) in enumerate(driver_rows)
                        ] or [{'name': 'Driver 1', 'max_hours': 2.5}],
                    }
                except ValueError as exc:
                    v_err.set(f'Invalid value: {exc}')
                    return
                try:
                    with open(PLAN_PATH, 'w') as f:
                        json.dump(plan, f, indent=2)
                    self._plan   = plan
                    self._stints = _calculate_stints(plan)
                    self._update_plan_display()
                    self.log(f'Race plan saved: {plan["name"]}')
                    dlg.destroy()
                except Exception as exc:
                    v_err.set(f'Save error: {exc}')

            ttk.Button(container, text='Finish Setup', style='Wizard.TButton',
                       command=finish_plan).pack(fill='x', pady=(8, 0))

        # ── Launch the wizard ────────────────────────────────────────────
        if start_at_plan and self._cfg.get('token'):
            show_plan_step()
        else:
            show_welcome()

    # ── Race plan editor modal ────────────────────────────────────────────────

    def _show_plan_editor(self):
        """Edit race plan — same form as wizard step 3 but pre-filled."""
        dlg = tk.Toplevel(self)
        dlg.title('Edit Race Plan')
        dlg.configure(bg=BG)
        dlg.resizable(True, True)
        dlg.grab_set()
        dlg.geometry('460x560')

        plan = self._plan or {}

        outer = tk.Frame(dlg, bg=BG)
        outer.pack(fill='both', expand=True, padx=20, pady=16)

        tk.Label(outer, text='Race Plan', bg=BG, fg=TEXT,
                 font=('Segoe UI', 13, 'bold')).pack(pady=(0, 10))

        # Scrollable area
        canvas     = tk.Canvas(outer, bg=BG, highlightthickness=0, height=380)
        scrollbar  = ttk.Scrollbar(outer, orient='vertical', command=canvas.yview)
        scroll_frm = tk.Frame(canvas, bg=BG)
        scroll_frm.bind('<Configure>', lambda e: canvas.configure(
            scrollregion=canvas.bbox('all')))
        canvas.create_window((0, 0), window=scroll_frm, anchor='nw')
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        def field(lbl, default):
            tk.Label(scroll_frm, text=lbl, bg=BG, fg=DIM,
                     font=('Segoe UI', 9), anchor='w').pack(fill='x', pady=(4, 0))
            var = tk.StringVar(value=str(default))
            ttk.Entry(scroll_frm, textvariable=var).pack(fill='x')
            return var

        v_race_name = field('Race Name', plan.get('name', 'My Race'))
        v_duration  = field('Duration (hours)', plan.get('race_duration_hrs', 2.5))
        v_lap_time  = field('Lap Time Target (seconds)', plan.get('lap_time_s', 92.0))
        v_capacity  = field('Fuel Capacity (litres)', plan.get('fuel_capacity_l', 18.5))
        v_fpl       = field('Fuel Per Lap (litres)', plan.get('fuel_per_lap_l', 0.92))
        v_pit_loss  = field('Pit Loss Time (seconds)', plan.get('pit_loss_s', 35.0))

        tk.Label(scroll_frm, text='Drivers', bg=BG, fg=DIM,
                 font=('Segoe UI', 9, 'bold'), anchor='w').pack(fill='x', pady=(10, 2))

        drivers_frame = tk.Frame(scroll_frm, bg=BG)
        drivers_frame.pack(fill='x')
        driver_rows = []

        def add_driver_row(name='Driver', hours='2.5'):
            rf = tk.Frame(drivers_frame, bg=BG)
            rf.pack(fill='x', pady=2)
            vn = tk.StringVar(value=name)
            vh = tk.StringVar(value=str(hours))
            ttk.Entry(rf, textvariable=vn, width=16).pack(side='left', padx=(0, 4))
            tk.Label(rf, text='Max hrs:', bg=BG, fg=DIM,
                     font=('Segoe UI', 8)).pack(side='left')
            ttk.Entry(rf, textvariable=vh, width=6).pack(side='left', padx=(2, 4))

            def remove():
                driver_rows.remove((vn, vh, rf))
                rf.destroy()

            tk.Button(rf, text='✕', bg=BG3, fg=ACCENT,
                      font=('Segoe UI', 8), relief='flat',
                      command=remove).pack(side='left')
            driver_rows.append((vn, vh, rf))

        for d in plan.get('drivers', [{'name': 'Driver 1', 'max_hours': 2.5}]):
            add_driver_row(d.get('name', 'Driver'), d.get('max_hours', 2.5))

        ttk.Button(scroll_frm, text='+ Add Driver', style='Browse.TButton',
                   command=lambda: add_driver_row()).pack(anchor='w', pady=4)

        v_err = tk.StringVar()
        tk.Label(outer, textvariable=v_err, bg=BG, fg=ACCENT,
                 font=('Segoe UI', 8), wraplength=420).pack(pady=2)

        def save_plan():
            try:
                new_plan = {
                    'name':              v_race_name.get().strip() or 'My Race',
                    'race_duration_hrs': float(v_duration.get()),
                    'lap_time_s':        float(v_lap_time.get()),
                    'fuel_capacity_l':   float(v_capacity.get()),
                    'fuel_per_lap_l':    float(v_fpl.get()),
                    'pit_loss_s':        float(v_pit_loss.get()),
                    'drivers': [
                        {'name': vn.get().strip() or f'Driver {i+1}',
                         'max_hours': float(vh.get())}
                        for i, (vn, vh, _) in enumerate(driver_rows)
                    ] or [{'name': 'Driver 1', 'max_hours': 2.5}],
                }
            except ValueError as exc:
                v_err.set(f'Invalid value: {exc}')
                return
            try:
                with open(PLAN_PATH, 'w') as f:
                    json.dump(new_plan, f, indent=2)
                self._plan   = new_plan
                self._stints = _calculate_stints(new_plan)
                self._update_plan_display()
                self.log(f'Race plan updated: {new_plan["name"]}')
                dlg.destroy()
            except Exception as exc:
                v_err.set(f'Save error: {exc}')

        ttk.Button(outer, text='Save Plan', style='Wizard.TButton',
                   command=save_plan).pack(fill='x', pady=(4, 0))
        ttk.Button(outer, text='Cancel', style='WizardSec.TButton',
                   command=dlg.destroy).pack(fill='x', pady=(4, 0))

    # ── Engineer control ─────────────────────────────────────────────────────

    def start_engineer(self):
        token = self._cfg.get('token', '')
        if not token:
            self.log('Not logged in — please complete setup first.')
            self._show_wizard()
            return

        # ── Load race plan (manual plan takes priority; auto-detection fills in if absent) ──
        plan = None
        if os.path.exists(PLAN_PATH):
            try:
                with open(PLAN_PATH) as f:
                    plan = json.load(f)
                required = ('race_duration_hrs', 'fuel_capacity_l', 'fuel_per_lap_l', 'lap_time_s')
                missing  = [k for k in required if k not in plan]
                if missing:
                    self.log(f'Race plan missing fields {missing} — will use auto-detection.')
                    plan = None
            except Exception as e:
                self.log(f'Could not load race plan ({e}) — will use auto-detection.')

        if plan:
            try:
                stints = _calculate_stints(plan)
            except Exception as e:
                self.log(f'Stint calculation error: {e}')
                return
            self._plan   = plan
            self._stints = stints
        else:
            self._plan   = {}
            self._stints = []
            self.log('[AUTO] No manual plan — will detect race from iRacing when you join a session.')

        self._update_plan_display()

        # ── Save relevant cfg prefs ───────────────────────────────────────
        self._cfg['fuel_unit']       = self.v_fuel_unit.get()
        self._cfg['spotter_enabled'] = self.v_spotter.get()
        save_config(self._cfg)

        # ── Determine voice availability ──────────────────────────────────
        voice_ok = SD_AVAILABLE and SCIPY_AVAILABLE and PYNPUT_AVAILABLE

        self._stop_evt.clear()
        self._running = True

        # ── Show/hide voice vs text input ─────────────────────────────────
        binding   = self._cfg.get('ptt_binding', DEFAULTS['ptt_binding'])
        btn_label = _binding_label(binding)
        if voice_ok:
            self.talk_label.config(
                fg=DIM, bg=BG, text=f'HOLD  {btn_label}  TO  TALK')
            self.text_input_frame.pack_forget()
            if binding.get('type') == 'joystick':
                self._start_joystick_listener(binding)
            else:
                self._start_keyboard_listener()
        else:
            self.talk_label.config(fg=BORDER, bg=BG, text='VOICE UNAVAILABLE — USE TEXT INPUT BELOW')
            self.text_input_frame.pack(fill='x', padx=14, pady=2)

        # ── Start threads ─────────────────────────────────────────────────
        self._telemetry_thread = TelemetryThread(self)
        self._telemetry_thread.start()

        threading.Thread(target=self._alert_loop, daemon=True).start()

        if self._cfg.get('spotter_enabled', True):
            try:
                from spotter import SpotterThread
                self._spotter_thread = SpotterThread(
                    speak_fn=self.speak,
                    log_fn=lambda msg: self.log(f'[SPOTTER] {msg}'),
                )
                self._spotter_thread.start()
                self.log('Spotter active.')
            except Exception as e:
                self.log(f'Spotter unavailable: {e}')

        self.start_btn.config(state='disabled')
        self.stop_btn.config(state='normal')

        # ── Log plan summary ──────────────────────────────────────────────
        self.log(f'─── Engineer started ─── {time.strftime("%H:%M:%S")} ───')
        if self._plan:
            p       = self._plan
            drivers = p.get('drivers', [])
            self.log(
                f'Plan  : {p.get("name", "?")}  |  '
                f'Duration: {p.get("race_duration_hrs", "?")}h  |  '
                f'Stints: {len(self._stints)}  |  '
                f'Drivers: {len(drivers)}'
            )
            for i, d in enumerate(drivers, 1):
                self.log(f'  Driver {i}: {d.get("name", "?")}  (max {d.get("max_hours", "?")}h)')
        else:
            self.log('Waiting for iRacing — race plan will be detected automatically.')

    def stop_engineer(self):
        self._stop_evt.set()
        self._running = False

        if self._telemetry_thread:
            self._telemetry_thread.stop()
            self._telemetry_thread = None

        if self._spotter_thread:
            self._spotter_thread.stop()
            self._spotter_thread = None

        self._stop_keyboard_listener()
        self._joystick_thread = None  # daemon thread — exits when _stop_evt is set

        self.start_btn.config(state='normal')
        self.stop_btn.config(state='disabled')
        self.set_status('stopped')
        binding   = self._cfg.get('ptt_binding', DEFAULTS['ptt_binding'])
        btn_label = _binding_label(binding)
        self.after(0, lambda: self.talk_label.config(
            fg=DIM, bg=BG, text=f'HOLD  {btn_label}  TO  TALK'))
        self.log('Engineer stopped.')

    # ── Stint panel refresh ───────────────────────────────────────────────────

    def _refresh_stint_panel(self):
        with self._ctx_lock:
            ctx = self._ctx
        if not ctx:
            for v in self._stint_vars.values():
                v.set('—')
            self._waiting_label.config(text='Waiting for iRacing…')
            return

        self._waiting_label.config(text='')
        live = ctx.get('live', {})
        cs   = live.get('current_stint', {})

        self._stint_vars['driver'].set(cs.get('driver_name', '—') or '—')
        self._stint_vars['lap'].set(str(live.get('current_lap', '—')))

        fuel_pct = live.get('fuel_pct')
        self._stint_vars['fuel'].set(f"{fuel_pct}%" if fuel_pct is not None else '—')

        laps_pit = live.get('laps_until_pit')
        self._stint_vars['pit'].set(str(laps_pit) if laps_pit is not None else '—')

    # ── Background: proactive alerts ─────────────────────────────────────────

    def _alert_loop(self):
        while not self._stop_evt.is_set():
            self._stop_evt.wait(5)
            if self._stop_evt.is_set():
                break
            with self._ctx_lock:
                ctx = self._ctx
            if not ctx:
                continue

            live      = ctx.get('live', {})
            now       = time.time()
            warn_laps = self._cfg.get('fuel_warning_laps', DEFAULTS['fuel_warning_laps'])

            laps_of_fuel   = live.get('laps_of_fuel')
            laps_until_pit = live.get('laps_until_pit')
            pit_status     = live.get('pit_window_status', '')
            pit_optimal    = live.get('pit_window_optimal', '?')

            # Fuel warning
            if (laps_of_fuel is not None
                    and laps_of_fuel <= warn_laps
                    and now - self._last_fuel_alert > 60):
                msg = (
                    f"Fuel warning. {laps_of_fuel:.1f} laps of fuel remaining. "
                    f"Pit window is lap {pit_optimal}."
                )
                self.speak(msg)
                self.log(f'[ALERT] {msg}')
                self._last_fuel_alert = now

            # Approaching pit window
            if (laps_until_pit is not None
                    and 0 < laps_until_pit <= 2
                    and now - self._last_pit_alert > 60):
                msg = f"Approaching pit window. {laps_until_pit} laps to pit."
                self.speak(msg)
                self.log(f'[ALERT] {msg}')
                self._last_pit_alert = now

            # Overdue
            if (pit_status == 'red'
                    and now - self._last_overdue_alert > 120):
                msg = "Overdue for pit stop. You are past the planned pit lap."
                self.speak(msg)
                self.log(f'[ALERT] {msg}')
                self._last_overdue_alert = now

    # ── Keyboard listener (push-to-talk) ─────────────────────────────────────

    def _ptt_key_matches(self, key) -> bool:
        """Return True if pynput key matches the configured PTT keyboard binding."""
        binding  = self._cfg.get('ptt_binding', DEFAULTS['ptt_binding'])
        key_name = binding.get('key', 'space')
        try:
            return key == getattr(pynput_keyboard.Key, key_name)
        except AttributeError:
            pass
        try:
            return bool(key.char and key.char.lower() == key_name.lower())
        except AttributeError:
            return False

    def _start_keyboard_listener(self):
        if not PYNPUT_AVAILABLE:
            return
        self._recording    = False
        self._audio_chunks = []
        self._ptt_down     = False

        def on_press(key):
            if not self._running:
                return
            if self._ptt_key_matches(key) and not self._ptt_down:
                self._ptt_down = True
                self._start_recording()

        def on_release(key):
            if not self._running:
                return
            if self._ptt_key_matches(key) and self._ptt_down:
                self._ptt_down = False
                self._stop_recording()

        self._kb_listener = pynput_keyboard.Listener(
            on_press=on_press, on_release=on_release)
        self._kb_listener.start()

    def _start_joystick_listener(self, binding: dict):
        """Poll pygame joystick for the bound button — runs as a daemon thread."""
        if not PYGAME_AVAILABLE or not SD_AVAILABLE:
            return
        self._recording    = False
        self._audio_chunks = []
        self._ptt_down     = False
        button_idx   = binding.get('button', 0)
        stored_name  = binding.get('device_name', '')
        stored_idx   = binding.get('device', 0)

        def _find_joystick():
            """Return the joystick matching the bound device (by name, then by index)."""
            count = pygame.joystick.get_count()
            if count == 0:
                return None
            # Try to match by name first (survives USB re-plug / index shifts)
            if stored_name:
                for i in range(count):
                    j = pygame.joystick.Joystick(i)
                    j.init()
                    if j.get_name() == stored_name:
                        return j
            # Fall back to stored index
            if stored_idx < count:
                j = pygame.joystick.Joystick(stored_idx)
                j.init()
                return j
            # Last resort: first available
            j = pygame.joystick.Joystick(0)
            j.init()
            return j

        def joy_loop():
            try:
                if not pygame.get_init():
                    pygame.init()
                pygame.joystick.init()
                joy = _find_joystick()
                if joy is None:
                    self.log('PTT: no joystick found — reconnect wheel and restart engineer')
                    return
                self.log(f'PTT: {joy.get_name()} button {button_idx}')

                while not self._stop_evt.is_set():
                    try:
                        pygame.event.pump()
                        btn_down = joy.get_button(button_idx)
                    except Exception:
                        # Device disconnected — try to re-find it
                        pygame.joystick.quit()
                        pygame.joystick.init()
                        joy = _find_joystick()
                        if joy is None:
                            time.sleep(2)
                            continue
                        btn_down = False

                    if btn_down and not self._ptt_down and self._running:
                        self._ptt_down = True
                        self._start_recording()
                    elif not btn_down and self._ptt_down:
                        self._ptt_down = False
                        self._stop_recording()
                    time.sleep(0.01)
            except Exception as e:
                self.log(f'Joystick PTT error: {e}')

        self._joystick_thread = threading.Thread(target=joy_loop, daemon=True)
        self._joystick_thread.start()

    def _stop_keyboard_listener(self):
        if self._kb_listener:
            try:
                self._kb_listener.stop()
            except Exception:
                pass
            self._kb_listener = None

    def _start_recording(self):
        if self._recording or not SD_AVAILABLE:
            return
        self._recording    = True
        self._audio_chunks = []
        self.after(0, lambda: self.talk_label.config(
            bg=ACCENT, fg='white', text='● RECORDING…'))

        def callback(indata, frames, t, status):
            if self._recording:
                self._audio_chunks.append(indata.copy())

        try:
            self._stream = sd.InputStream(
                samplerate=16000, channels=1, dtype='float32',
                callback=callback,
            )
            self._stream.start()
        except Exception as e:
            self.log(f'Audio error: {e}')
            self._recording = False
            self.after(0, self._reset_talk_label)

    def _reset_talk_label(self):
        binding   = self._cfg.get('ptt_binding', DEFAULTS['ptt_binding'])
        btn_label = _binding_label(binding)
        self.talk_label.config(bg=BG, fg=DIM, text=f'HOLD  {btn_label}  TO  TALK')

    def _stop_recording(self):
        if not self._recording:
            return
        self._recording = False
        self.after(0, self._reset_talk_label)
        try:
            self._stream.stop()
            self._stream.close()
        except Exception:
            pass

        chunks = self._audio_chunks
        if not chunks:
            return

        def _save_and_process():
            try:
                audio       = np.concatenate(chunks, axis=0).flatten()
                wav_path    = tempfile.mktemp(suffix='.wav')
                audio_int16 = (audio * 32767).astype(np.int16)
                wavfile.write(wav_path, 16000, audio_int16)
                self._process_voice(wav_path)
            except Exception as e:
                self.log(f'Recording save error: {e}')

        threading.Thread(target=_save_and_process, daemon=True).start()

    # ── Voice processing — via backend ────────────────────────────────────────

    def _process_voice(self, wav_path: str):
        token = self._cfg.get('token', '')
        if not token:
            self.log('Not logged in — cannot process voice')
            return
        try:
            with open(wav_path, 'rb') as f:
                r = requests.post(
                    f'{BACKEND_URL}/engineer/transcribe',
                    data={'token': token},
                    files={'audio': ('audio.wav', f, 'audio/wav')},
                    timeout=15,
                )
            if not r.ok:
                self.log(f'Transcription error: {r.text[:80]}')
                return
            question = r.json().get('transcript', '').strip()
            if not question:
                return
            self.log(f'You: "{question}"')
            self._ask_engineer(question)
        except Exception as e:
            self.log(f'Voice error: {e}')
        finally:
            try:
                os.remove(wav_path)
            except Exception:
                pass

    # ── Text fallback ─────────────────────────────────────────────────────────

    def _ask_from_text(self):
        question = self.v_question.get().strip()
        if not question:
            return
        self.v_question.set('')
        self.log(f'You: "{question}"')
        threading.Thread(target=self._ask_engineer, args=(question,), daemon=True).start()

    # ── AI query — via backend ────────────────────────────────────────────────

    def _ask_engineer(self, question: str):
        token = self._cfg.get('token', '')
        if not token:
            self.log('Not logged in')
            return
        with self._ctx_lock:
            ctx = self._ctx
        system_prompt = self._build_system_prompt(ctx) if ctx else ''
        try:
            r = requests.post(
                f'{BACKEND_URL}/engineer/ask',
                json={'token': token, 'system_prompt': system_prompt, 'question': question},
                timeout=15,
            )
            data = r.json()
            if not r.ok:
                if data.get('quota_exceeded'):
                    self.log('Daily query limit reached. Resets midnight UTC.')
                    self.speak('Daily query limit reached.')
                else:
                    self.log(f'Engineer error: {data.get("error", "unknown")}')
                return
            answer    = data.get('answer', '')
            remaining = data.get('query_limit', 50) - data.get('queries_today', 0)
            self._queries_today = data.get('queries_today', self._queries_today)
            self._query_limit   = data.get('query_limit', self._query_limit)
            self.log(f'Engineer: {answer}')
            self.log(f'  ({remaining} queries remaining today)')
            self.after(0, lambda: self.v_queries.set(
                f'{self._queries_today} / {self._query_limit} today  ({remaining} remaining)'
            ))
            self.after(0, lambda: self._append_qa(question, answer))
            self.speak(answer)
        except Exception as e:
            self.log(f'Engineer error: {e}')

    def _build_system_prompt(self, ctx: dict) -> str:
        plan = ctx.get('plan', {})
        live = ctx.get('live', {})
        tele = ctx.get('telemetry', {})

        cs = live.get('current_stint', {})
        ns = live.get('next_stint', {})

        def safe_float(v, fmt='.1f'):
            try:
                return format(float(v), fmt)
            except (TypeError, ValueError):
                return str(v) if v is not None else '?'

        lines = [
            "You are a professional endurance racing engineer. Answer concisely — "
            "1-3 sentences maximum unless asked for detail. Be direct and specific with numbers.",
            "",
            f"RACE: {plan.get('name', 'Unknown')} | Duration: {plan.get('race_duration_hrs', '?')}h",
            f"LAP: {live.get('current_lap', '?')} | DRIVER: {cs.get('driver_name', '?')} "
            f"| STINT: {cs.get('stint_num', '?')} of {plan.get('total_stints', '?')}",
            f"FUEL: {safe_float(live.get('fuel_remaining_l'))}L remaining | "
            f"{safe_float(live.get('laps_of_fuel'))} laps | {live.get('fuel_pct', '?')}%",
            f"PIT WINDOW: lap {live.get('pit_window_optimal', '?')} "
            f"(last safe: {live.get('pit_window_last', '?')}) | "
            f"{live.get('laps_until_pit', '?')} laps away | "
            f"Status: {str(live.get('pit_window_status', '?')).upper()}",
        ]

        if ns:
            lines.append(
                f"NEXT DRIVER: {ns.get('driver_name', '?')} | Fuel load: {ns.get('fuel_load', '?')}L"
            )

        fd = tele.get('fuel_delta', {})
        if fd.get('avg_actual_fpl'):
            planned_fpl = plan.get('fuel_per_lap_l', '?')
            lines.append(
                f"FUEL DELTA: actual {fd['avg_actual_fpl']:.3f}L/lap vs planned {planned_fpl}L/lap"
            )

        lines.append("")
        lines.append("STINT PLAN SUMMARY:")
        for s in plan.get('stints', [])[:plan.get('total_stints', 99)]:
            marker  = "-> " if s.get('stint_num') == cs.get('stint_num') else "   "
            pit_str = f"pit lap {s['pit_lap']}" if s.get('pit_lap') else "FINAL"
            lines.append(
                f"{marker}Stint {s['stint_num']}: {s.get('driver_name', '?')} "
                f"laps {s['start_lap']}-{s['end_lap']} ({pit_str}) {s['fuel_load']}L"
            )

        return "\n".join(lines)

    # ── Q&A display ───────────────────────────────────────────────────────────

    def _append_qa(self, question: str, answer: str):
        self.qa_box.config(state='normal')
        self.qa_box.insert('end', f'Q: {question}\nA: {answer}\n\n')
        self.qa_box.see('end')
        lines = int(self.qa_box.index('end-1c').split('.')[0])
        if lines > 20:
            self.qa_box.delete('1.0', f'{lines - 20}.0')
        self.qa_box.config(state='disabled')

    # ── TTS ───────────────────────────────────────────────────────────────────

    def speak(self, text: str):
        """Speak text via pyttsx3 in a background thread (non-blocking)."""
        if not TTS_AVAILABLE:
            return
        def _do():
            try:
                engine = pyttsx3.init()
                engine.setProperty('rate', 175)
                engine.say(text)
                engine.runAndWait()
            except Exception as e:
                self.log(f'TTS error: {e}')
        threading.Thread(target=_do, daemon=True).start()

    # ── Status helpers ────────────────────────────────────────────────────────

    def set_status(self, status: str):
        colors = {
            'connected':  (GREEN,  'Connected — iRacing live'),
            'error':      (ACCENT, 'Connection error — retrying'),
            'stopped':    (BORDER, 'Stopped'),
            'connecting': (YELLOW, 'Connecting to iRacing…'),
        }
        color, text = colors.get(status, (BORDER, status))
        self.after(0, lambda: (
            self.status_dot.config(fg=color),
            self.status_label.config(text=text, fg=color),
        ))

    def log(self, msg: str):
        def _append():
            self.log_box.config(state='normal')
            self.log_box.insert('end', msg + '\n')
            self.log_box.see('end')
            lines = int(self.log_box.index('end-1c').split('.')[0])
            if lines > 500:
                self.log_box.delete('1.0', f'{lines - 500}.0')
            self.log_box.config(state='disabled')
        self.after(0, _append)

    def on_close(self):
        self.stop_engineer()
        self.destroy()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    app = App()
    app.mainloop()
