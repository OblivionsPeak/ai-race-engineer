"""
Neural Racing Performance — AI Race Engineer (Standalone)
=======================================================
Push-to-talk voice assistant + proactive alerts via backend proxy.
Reads iRacing telemetry directly via pyirsdk. No direct Anthropic/OpenAI
API keys required — all AI queries and transcription go through the backend.

Python 3.10+ is the only requirement — the script installs everything else itself.
"""

import json
import math
import os
import subprocess
import sys

# ── Backend server URL — update this before building the public EXE ──────────
BACKEND_URL = "https://endurance-planner-production.up.railway.app"
# ─────────────────────────────────────────────────────────────────────────────

VERSION     = "1.1.31"
GITHUB_REPO = "OblivionsPeak/ai-race-engineer"

# ── Auto-install missing packages (script mode only — frozen EXE bundles all) ─
def _ensure(package, import_name=None):
    if getattr(sys, 'frozen', False):
        return  # running as PyInstaller EXE — packages are already bundled
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
_ensure('edge-tts', 'edge_tts')
# ── Now safe to import ───────────────────────────────────────────────────────

import asyncio
import queue
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
    import edge_tts
    EDGE_TTS_AVAILABLE = True
except ImportError:
    EDGE_TTS_AVAILABLE = False

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
BG     = '#050d12'
BG2    = '#091820'
BG3    = '#0d2430'
BORDER = '#174858'
ACCENT = '#00c8d4'
GREEN  = '#00d890'
YELLOW = '#f0c040'
TEXT   = '#eaf7f9'
DIM    = '#68b8c4'
CYAN   = '#48f8f8'


# ---------------------------------------------------------------------------
# Config persistence  (AppData)
# ---------------------------------------------------------------------------
APPDATA_DIR  = os.path.join(os.environ.get('APPDATA', os.path.expanduser('~')), 'AIRaceEngineer')
CONFIG_PATH  = os.path.join(APPDATA_DIR, 'config.json')
PLAN_PATH    = os.path.join(APPDATA_DIR, 'race_plan.json')
SESSIONS_DIR = os.path.join(APPDATA_DIR, 'sessions')

PERSONALITIES = {
    'Professional Engineer': 'professional',
    'Intense & Aggressive':  'aggressive',
    'Friendly Coach':        'friendly',
}

PERSONALITY_PROMPTS = {
    'professional': (
        "You are a calm, precise, data-driven race engineer. Speak like a real F1 engineer — "
        "concise, factual, no fluff. Use numbers. 1-3 sentences unless detail is requested."
    ),
    'aggressive': (
        "You are an intense, fired-up race engineer. Direct, urgent, passionate. Push the driver "
        "hard. Short punchy sentences. 1-3 sentences unless detail is requested."
    ),
    'friendly': (
        "You are an encouraging, friendly race coach. Positive, supportive, motivational. "
        "Warm but still data-driven. 1-3 sentences unless detail is requested."
    ),
}
DEFAULT_PERSONALITY = 'professional'

EDGE_VOICES = {
    'en-US-AriaNeural    (US Female)':    'en-US-AriaNeural',
    'en-US-GuyNeural     (US Male)':      'en-US-GuyNeural',
    'en-GB-SoniaNeural   (UK Female)':    'en-GB-SoniaNeural',
    'en-GB-RyanNeural    (UK Male)':      'en-GB-RyanNeural',
    'en-AU-NatashaNeural (AU Female)':    'en-AU-NatashaNeural',
    'en-AU-WilliamNeural (AU Male)':      'en-AU-WilliamNeural',
    'en-IE-EmilyNeural   (Irish Female)': 'en-IE-EmilyNeural',
    'SAPI5 (built-in, offline)':          'sapi5',
}
DEFAULT_VOICE = 'en-GB-RyanNeural'

DEFAULTS = {
    'token':             '',
    'display_name':      '',
    'fuel_warning_laps': 3,
    'fuel_unit':         'gal',
    'ptt_binding':       {'type': 'keyboard', 'key': 'space'},
    'spotter_enabled':   True,
    'tts_voice':         DEFAULT_VOICE,
    'personality':       DEFAULT_PERSONALITY,
    'units_system':      'metric',
    'checkin_laps':      5,
    'checkin_mins':      0,
    'tts_rate':          1.0,
    'listen_mode':       'ptt',   # 'ptt', 'vad', or 'wake'
    'vad_sensitivity':   0.02,    # RMS threshold for voice activity detection
    'wake_word':         'hey engineer',
}

def _c_to_f(c: float) -> float:  return c * 9 / 5 + 32

# Create AppData directory on startup if it doesn't exist
os.makedirs(APPDATA_DIR, exist_ok=True)
os.makedirs(SESSIONS_DIR, exist_ok=True)


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
    'fuel_capacity_l':   50.0,
    'fuel_per_lap_l':    2.5,
    'lap_time_s':        120.0,
    'pit_loss_s':        35.0,
    'drivers': [
        {'name': 'Driver 1', 'max_hours': 2.5},
    ],
    'championship_context': {
        'enabled':               False,
        'championship_name':     '',
        'current_points':        0,
        'points_leader_points':  0,
        'points_leader_name':    '',
        'points_per_position':   [25, 18, 15, 12, 10, 8, 6, 4, 2, 1],
        'races_remaining':       10,
        'race_number':           1,
    },
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
    except Exception as e:
        print(f'[NRP] Config save failed: {e}', file=sys.stderr)


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
            'track':             track_name,
            'car':               car_name,
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
        last_fpl_lap            = 0
        fuel_at_lap_end         = None   # fuel level captured at each lap boundary (not every tick)
        fuel_history            = []
        auto_plan_detected      = False
        last_pit_repair_left    = 0.0    # track changes to fire proactive damage alerts
        last_engine_warnings    = 0      # track new engine warning bits
        last_meatball           = False  # track meatball (serviceable) flag transitions
        fast_tick               = 0      # counts fast-path iterations to gate slow path

        # Per-connection mutable state (reset implicitly on reconnect)
        sector_s1_delta: float | None = None  # delta vs session best at ~33% of lap
        sector_s2_delta: float | None = None  # delta vs session best at ~67% of lap
        sector_s3_delta: float | None = None  # delta vs session best at ~92% of lap
        opp_prev_on_pit: dict = {}            # {car_idx: bool}
        opp_pit_entry_lap: dict = {}          # {car_idx: lap_num when entered pit}
        dynamics_buffer: list = []            # per-tick tuples (throttle, brake, lat_accel, yaw_rate, speed_ms)
        player_on_pit_prev: bool = False
        player_pit_entry_time: float | None = None
        player_pit_entry_fuel: float | None = None
        pending_lap_complete: tuple | None = None  # deferred until LapLastLapTime updates

        def _safe(v):
            try: return round(float(v), 2) if v is not None else None
            except (TypeError, ValueError): return None

        def _find_car_at_pos(pos, positions, f2times, excl_idx, names, on_track=None):
            for idx, p in enumerate(positions):
                if p == pos and idx != excl_idx:
                    if on_track and idx < len(on_track) and not on_track[idx]:
                        continue
                    gap = f2times[idx] if idx < len(f2times) else None
                    # Negative F2Time is iRacing's sentinel for invalid/not-yet-set data
                    if gap is None or gap < 0:
                        return None
                    return {'position': pos, 'name': names.get(idx, '?'), 'gap': gap}
            return None

        while not self._stop.is_set():
            try:
                if not ir.is_initialized or not ir.is_connected:
                    self._app.set_status('connecting')
                    auto_plan_detected = False
                    # Mark existing ctx stale so the AI knows telemetry is unavailable
                    with self._app._ctx_lock:
                        if self._app._ctx:
                            self._app._ctx['telemetry']['stale'] = True
                        else:
                            self._app._ctx = None
                    ir.startup()
                    self._stop.wait(2)
                    continue

                ir.freeze_var_buffer_latest()

                # ── FAST PATH — runs every tick at ~60 Hz ─────────────────────────
                # Read only the variables needed for real-time telemetry, flags, and
                # sector/dynamics capture. Everything else waits for the slow path.

                lap_dist_pct    = float(ir['LapDistPct']                 or 0.0)
                throttle_in     = float(ir['Throttle']                   or 0.0)
                brake_in        = float(ir['Brake']                      or 0.0)
                lat_accel_in    = float(ir['LatAccel']                   or 0.0)
                yaw_rate_in     = float(ir['YawRate']                    or 0.0)
                speed_ms_in     = float(ir['Speed']                      or 0.0)
                steering_in     = float(ir['SteeringWheelAngle']         or 0.0)
                steer_max       = float(ir['SteeringWheelAngleMax']      or 3.14)
                gear_in         = int(ir['Gear']                         or 0)
                rpm_in          = float(ir['RPM']                        or 0.0)
                current_lap     = ir['Lap']                              or 0
                session_time    = ir['SessionTime']                      or 0.0
                lap_delta_best  = ir['LapDeltaToSessionBestLap']
                lap_delta_ok    = bool(ir['LapDeltaToSessionBestLap_OK'] or False)
                session_flags   = int(ir['SessionFlags']                 or 0)
                is_on_track     = bool(ir['IsOnTrack']                   or False)
                pit_repair_left = float(ir['PitRepairLeft']              or 0.0)
                engine_warnings_raw = int(ir['EngineWarnings']           or 0)

                # Update pit-wall broadcast frame (broadcast thread throttles to 20 fps)
                self._app._tele_frame = {
                    't': round(throttle_in, 3),
                    'b': round(brake_in, 3),
                    's': round(steering_in / max(steer_max, 0.01), 3),
                    'v': round(speed_ms_in, 1),
                    'g': gear_in,
                    'r': int(rpm_in),
                    'p': round(lap_dist_pct, 4),
                    'l': current_lap,
                    'ts': round(session_time, 2),
                }

                # Sector delta capture — needs high resolution to hit the windows reliably
                if lap_dist_pct < 0.05:
                    sector_s1_delta = None
                    sector_s2_delta = None
                    sector_s3_delta = None
                if (0.31 < lap_dist_pct < 0.37 and sector_s1_delta is None
                        and lap_delta_ok and lap_delta_best is not None):
                    sector_s1_delta = round(float(lap_delta_best), 3)
                    self._app.after(0, lambda d=sector_s1_delta:
                        self._app._on_sector_delta('S1', d))
                if (0.63 < lap_dist_pct < 0.69 and sector_s2_delta is None
                        and lap_delta_ok and lap_delta_best is not None):
                    sector_s2_delta = round(float(lap_delta_best), 3)
                    self._app.after(0, lambda d=sector_s2_delta:
                        self._app._on_sector_delta('S2', d))
                if (0.92 < lap_dist_pct < 0.98 and sector_s3_delta is None
                        and lap_delta_ok and lap_delta_best is not None):
                    sector_s3_delta = round(float(lap_delta_best), 3)
                    self._app.after(0, lambda d=sector_s3_delta:
                        self._app._on_sector_delta('S3', d))

                # Dynamics accumulation for end-of-lap handling analysis
                dynamics_buffer.append(
                    (throttle_in, brake_in, lat_accel_in, yaw_rate_in, speed_ms_in))

                # Proactive damage alert — fires immediately when PitRepairLeft increases
                if pit_repair_left > last_pit_repair_left + 1.0 and is_on_track:
                    _opt = float(ir['PitOptRepairLeft'] or 0.0)
                    _fra = ir['FastRepairAvailable']
                    self._app.after(0, lambda r=pit_repair_left, o=_opt, fr=_fra:
                        self._app._on_damage_detected(r, o, fr))
                last_pit_repair_left = pit_repair_left

                # Meatball flag (irsdk_serviceable = 0x40000) — mechanical issue
                meatball_now = bool(session_flags & 0x40000)
                if meatball_now and not last_meatball:
                    self._app.after(0, self._app._on_meatball_flag)
                last_meatball = meatball_now

                # Engine warning alerts — fire on new bits, not every tick
                new_warn_bits = engine_warnings_raw & ~last_engine_warnings
                if new_warn_bits and is_on_track:
                    self._app.after(0, lambda b=new_warn_bits:
                        self._app._on_engine_warning(b))
                last_engine_warnings = engine_warnings_raw

                # ── SLOW PATH — every 30 fast ticks (~2 Hz at 60 Hz poll) ──────────
                # Fuel calcs, opponent parsing, ctx build — don't need every frame.
                fast_tick += 1
                if fast_tick % 30 != 0:
                    self._stop.wait(0.016)
                    continue

                # Auto-detect race plan once per connection if no manual plan exists
                if not auto_plan_detected:
                    auto_plan = _build_auto_plan_from_ir(ir)
                    if auto_plan:
                        auto_plan_detected = True
                        self._app.after(0, lambda p=auto_plan: self._app._apply_auto_plan(p))

                with self._app._ctx_lock:
                    plan   = self._app._plan
                    stints = self._app._stints
                fuel_unit = self._app._cfg.get('fuel_unit', 'gal')

                fuel_raw       = ir['FuelLevel']
                lap_last       = ir['LapLastLapTime']            or 0.0
                lap_completed  = ir['LapCompleted']              or 0
                session_type   = ir['SessionType']               or ''
                time_remain_s  = ir['SessionTimeRemain']
                laps_remain    = ir['SessionLapsRemainEx']
                incidents      = ir['PlayerCarTeamIncidentCount'] or 0
                air_temp_c     = ir['AirTemp']
                track_temp_c   = ir['TrackTempCrew']
                weather_wet    = bool(ir['WeatherDeclaredWet'] or False)
                track_wetness  = int(ir['TrackWetness']          or 0)
                player_on_pit  = bool(ir['OnPitRoad']            or False)
                pit_sv_flags   = int(ir['PitSvFlags']            or 0)

                # Damage / engine vitals (full reads on slow path for ctx)
                pit_opt_repair_left = float(ir['PitOptRepairLeft']  or 0.0)
                fast_repair_avail   = ir['FastRepairAvailable']
                fast_repair_used    = ir['FastRepairUsed']           or 0
                water_temp_c        = ir['WaterTemp']
                oil_temp_c          = ir['OilTemp']
                oil_press_kpa       = ir['OilPress']
                oil_level_l         = ir['OilLevel']

                def _tire_wear(corner):
                    try:
                        vals = [x for x in (ir[f'{corner}wearL'], ir[f'{corner}wearM'],
                                             ir[f'{corner}wearR']) if x is not None]
                        return round(sum(vals) / len(vals), 3) if vals else None
                    except Exception:
                        return None

                tire_wear = {'LF': _tire_wear('LF'), 'RF': _tire_wear('RF'),
                             'LR': _tire_wear('LR'), 'RR': _tire_wear('RR')}

                engine_warnings = {
                    'water_temp':    bool(engine_warnings_raw & 0x01),
                    'fuel_pressure': bool(engine_warnings_raw & 0x02),
                    'oil_pressure':  bool(engine_warnings_raw & 0x04),
                    'stalled':       bool(engine_warnings_raw & 0x08),
                }

                # Opponent data
                car_idx_positions = ir['CarIdxPosition']   or []
                car_idx_f2time    = ir['CarIdxF2Time']      or []
                car_idx_on_pit    = ir['CarIdxOnPitRoad']   or []
                car_idx_on_track  = ir['CarIdxOnTrack']     or []
                my_car_idx        = ir['PlayerCarIdx']       or 0
                driver_info       = ir['DriverInfo']         or {}

                fuel = fuel_raw

                # Rolling fuel-per-lap delta (lap-boundary triggered)
                fuel_delta = {}
                if lap_completed > last_fpl_lap:
                    if fuel_at_lap_end is not None and fuel is not None:
                        actual_fpl = round(fuel_at_lap_end - fuel, 4)
                        if 0.05 < actual_fpl < 10.0:
                            fuel_history.append(actual_fpl)
                            fuel_history = fuel_history[-10:]
                            avg_fpl = round(sum(fuel_history) / len(fuel_history), 4)
                            fuel_delta = {
                                'avg_actual_fpl':  avg_fpl,
                                'last_actual_fpl': actual_fpl,
                                'history':         list(fuel_history),
                            }
                            if len(fuel_history) >= 3 and self._app._plan.get('auto_detected'):
                                self._app.after(0, lambda f=avg_fpl: self._app._update_auto_fpl(f))
                    else:
                        actual_fpl = 0.0
                    if lap_last > 0:
                        _fpl_for_coach = actual_fpl if (0.05 < actual_fpl < 10.0) else None
                        _sd: dict = {}
                        if sector_s1_delta is not None: _sd['s1'] = sector_s1_delta
                        if sector_s2_delta is not None: _sd['s2'] = sector_s2_delta
                        if sector_s3_delta is not None: _sd['s3'] = sector_s3_delta
                        _dyn: dict = {}
                        if dynamics_buffer:
                            _n = len(dynamics_buffer)
                            _dyn = {
                                'avg_throttle': round(sum(d[0] for d in dynamics_buffer) / _n, 2),
                                'avg_brake':    round(sum(d[1] for d in dynamics_buffer) / _n, 2),
                                # Oversteer: rotating fast (yaw > 0.4 rad/s) with low throttle at speed
                                # Captures lift-off / brake oversteer; excludes power oversteer & low-speed hairpins
                                'oversteer':    round(sum(1 for d in dynamics_buffer
                                                    if abs(d[3]) > 0.4
                                                    and d[0] < 0.3
                                                    and d[4] > 10) / _n * 100, 1),
                                # Understeer: car has lateral load (in a corner) but is not rotating
                                # abs(lat_accel) > 1.0 m/s² = actually cornering; yaw < 0.15 rad/s = plowing
                                'understeer':   round(sum(1 for d in dynamics_buffer
                                                    if abs(d[2]) > 1.0
                                                    and abs(d[3]) < 0.15
                                                    and d[4] > 15) / _n * 100, 1),
                            }
                        dynamics_buffer.clear()
                        # 2 slow ticks (~1 s) lets LapLastLapTime settle before coaching fires
                        pending_lap_complete = (lap_completed, _fpl_for_coach,
                                                dict(_sd), dict(_dyn), session_type, 2)
                    if fuel is not None:
                        fuel_at_lap_end = fuel
                    last_fpl_lap = lap_completed

                # Fire deferred lap-complete after 2 slow-path ticks
                if pending_lap_complete is not None:
                    *_data, _ticks = pending_lap_complete
                    if _ticks > 1:
                        pending_lap_complete = (*_data, _ticks - 1)
                    else:
                        _plc, _pfpl, _psd, _pdy, _pst = _data
                        pending_lap_complete = None
                        if lap_last > 0:
                            self._app.after(0, lambda lt=lap_last, lc=_plc, fp=_pfpl, st=_pst,
                                            sd=dict(_psd), dy=dict(_pdy):
                                self._app._on_lap_complete(lc, lt, fp, st, sd, dy))

                if not fuel_delta and fuel_history:
                    _avg_fpl = round(sum(fuel_history) / len(fuel_history), 4)
                    fuel_delta = {
                        'avg_actual_fpl':  _avg_fpl,
                        'last_actual_fpl': fuel_history[-1],
                        'history':         list(fuel_history),
                    }

                # Player pit stop tracking
                if player_on_pit and not player_on_pit_prev:
                    player_pit_entry_time = session_time
                    player_pit_entry_fuel = fuel
                elif not player_on_pit and player_on_pit_prev and player_pit_entry_time is not None:
                    _dur = session_time - player_pit_entry_time
                    _fa  = max(fuel - (player_pit_entry_fuel or fuel), 0.0)
                    _flg = pit_sv_flags
                    if 5.0 < _dur < 120.0:
                        self._app.after(0, lambda d=_dur, fa=_fa, fl=_flg:
                            self._app._on_pit_stop_complete(d, fa, fl))
                    player_pit_entry_time = None
                    player_pit_entry_fuel = None
                player_on_pit_prev = player_on_pit

                # Opponent parsing
                opponents = {}
                my_position = None
                try:
                    my_pos      = car_idx_positions[my_car_idx] if my_car_idx < len(car_idx_positions) else 0
                    my_position = my_pos if my_pos > 0 else None
                    drivers     = driver_info.get('Drivers', []) or []
                    idx_map     = {d.get('CarIdx', -1): d.get('UserName', '?') for d in drivers}
                    if my_pos > 1:
                        ahead = _find_car_at_pos(my_pos - 1, car_idx_positions, car_idx_f2time,
                                                 my_car_idx, idx_map, car_idx_on_track)
                        if ahead:
                            opponents['ahead'] = ahead
                    behind = _find_car_at_pos(my_pos + 1, car_idx_positions, car_idx_f2time,
                                              my_car_idx, idx_map, car_idx_on_track)
                    if behind:
                        opponents['behind'] = behind
                    opponents['my_position'] = my_position
                    for _ci, _on_pit in enumerate(car_idx_on_pit):
                        if _ci == my_car_idx:
                            continue
                        _was = opp_prev_on_pit.get(_ci, False)
                        if bool(_on_pit) and not _was:
                            opp_pit_entry_lap[_ci] = lap_completed
                        elif not bool(_on_pit) and _was:
                            _entry = opp_pit_entry_lap.pop(_ci, None)
                            if _entry is not None:
                                _opos = car_idx_positions[_ci] if _ci < len(car_idx_positions) else 0
                                if my_position and _opos and abs(_opos - my_position) <= 5:
                                    self._app.after(0, lambda n=idx_map.get(_ci, '?'),
                                                    p=_opos, el=_entry:
                                        self._app._on_opponent_pit_exit(n, p, el))
                        opp_prev_on_pit[_ci] = bool(_on_pit)
                except Exception as e:
                    self._app.log(f'Opponents parse error: {e}')

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
                        'current_lap':        current_lap,
                        'fuel_level':         round(fuel, 3) if fuel is not None else None,
                        'last_lap_time_s':    round(lap_last, 3) if lap_last > 0 else None,
                        'session_time_s':     round(session_time, 1),
                        'fuel_delta':         fuel_delta,
                        'fuel_laps_measured': len(fuel_history),
                        'opponents':          opponents,
                        'incidents':          incidents,
                        'on_pit_road':        player_on_pit,
                        'is_on_track':        is_on_track,
                        'stale':              False,
                    },
                    'damage': {
                        'pit_repair_s':      round(pit_repair_left, 1),
                        'pit_opt_repair_s':  round(pit_opt_repair_left, 1),
                        'fast_repair_avail': int(fast_repair_avail) if fast_repair_avail is not None else None,
                        'fast_repair_used':  fast_repair_used,
                        'engine_warnings':   engine_warnings,
                        'water_temp_c':      round(water_temp_c, 1) if water_temp_c is not None else None,
                        'oil_temp_c':        round(oil_temp_c, 1) if oil_temp_c is not None else None,
                        'oil_press_kpa':     round(oil_press_kpa, 1) if oil_press_kpa is not None else None,
                        'oil_level_l':       round(oil_level_l, 2) if oil_level_l is not None else None,
                        'tire_wear':         tire_wear,
                    },
                    'session': {
                        'type':             session_type,
                        'time_remaining_s': _safe(time_remain_s),
                        'laps_remaining':   int(laps_remain) if laps_remain is not None and laps_remain >= 0 else None,
                    },
                    'weather': {
                        'air_temp_c':    _safe(air_temp_c),
                        'track_temp_c':  _safe(track_temp_c),
                        'wet':           weather_wet,
                        'track_wetness': track_wetness,
                    },
                    'session_flags': {
                        'blue':       bool(session_flags & 0x0020),   # irsdk_blue
                        'yellow':     bool(session_flags & 0x0008),   # irsdk_yellow
                        'caution':    bool(session_flags & 0x4000),   # irsdk_caution
                        'black':      bool(session_flags & 0x10000),  # irsdk_black (DQ)
                        'meatball':   bool(session_flags & 0x40000),  # irsdk_serviceable
                    },
                }

                with self._app._ctx_lock:
                    self._app._ctx = ctx
                self._app.set_status('connected')
                self._app.after(0, self._app._refresh_stint_panel)

            except Exception as e:
                self._app.log(f'Telemetry error: {e}')

            self._stop.wait(0.016)

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
    then exit. Waits for the current PID to fully exit before moving,
    which prevents the 'Failed to load Python DLL' race condition.
    """
    current_exe = sys.executable if getattr(sys, 'frozen', False) else None
    if not current_exe:
        return  # running as a .py script — skip

    pid      = os.getpid()
    exe_dir  = os.path.dirname(current_exe)
    bat_path = os.path.join(tempfile.gettempdir(), '_aire_update.bat')
    bat = (
        '@echo off\n'
        # Poll until the old PID is gone (checks every 500 ms, up to 30 s)
        f':wait\n'
        f'tasklist /fi "pid eq {pid}" 2>nul | find "{pid}" >nul\n'
        f'if not errorlevel 1 (\n'
        f'    timeout /t 1 /nobreak >nul\n'
        f'    goto wait\n'
        f')\n'
        f'move /y "{new_exe_path}" "{current_exe}"\n'
        f'del /f /q "{exe_dir}\\_nrp_update_*.exe" 2>nul\n'
        # Give Windows Defender time to finish scanning the replaced EXE before launching
        f'timeout /t 8 /nobreak >nul\n'
        # PowerShell Start-Process is more reliable than cmd start for launching
        # freshly written EXEs — cmd start can silently fail if Defender is still scanning
        f'powershell -Command "Start-Process \'{current_exe}\'"\n'
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
# Callout Manager — single dedup/cooldown gate for all spoken callouts.
# Both the alert loop and SpotterThread route through here so the same event
# cannot fire twice regardless of which subsystem detects it first.
# ---------------------------------------------------------------------------
class CalloutManager:
    def __init__(self, speak_fn, log_fn=None):
        self._speak = speak_fn
        self._log   = log_fn or (lambda _: None)
        self._last: dict = {}
        self._lock  = threading.Lock()

    def submit(self, key: str, message: str, cooldown_s: float = 0.0) -> bool:
        now = time.time()
        with self._lock:
            if now - self._last.get(key, 0.0) < cooldown_s:
                return False
            self._last[key] = now
        self._speak(message)
        return True

    def reset(self):
        with self._lock:
            self._last.clear()


# ---------------------------------------------------------------------------
# Main App
# ---------------------------------------------------------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('Neural Racing Performance')
        self.configure(bg=BG)
        self.resizable(True, True)
        self.minsize(560, 720)
        # Set window + taskbar icon
        try:
            _ico = os.path.join(
                getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__))),
                'ai_race_engineer.ico')
            if os.path.exists(_ico):
                self.iconbitmap(_ico)
        except Exception:
            pass

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
        self._callout_mgr: CalloutManager | None = None
        self._last_fuel_alert    = 0.0
        self._ptt_down           = False
        self._last_pit_alert     = 0.0
        self._last_overdue_alert = 0.0
        self._joystick_thread: threading.Thread | None = None
        self._queries_today      = 0
        self._query_limit        = 50
        self._display_name       = self._cfg.get('display_name', '')
        self._convo_history: list = []
        self._session_notes: list = []
        self._session_memory_summary: str = ''
        self._server_session_id: int | None = None
        self._session_started   = False
        self._session_best_lap: float = 0.0
        self._lap_times_this_session: list = []
        self._last_coached_lap: int = 0
        self._last_handling_coached_lap: int = 0
        self._total_laps_this_session: int = 0
        self._coaching_in_flight: bool = False
        self._last_strategy_lap: int = 0
        self._last_pit_briefed_stint: int = 0
        self._last_fuel_save_lap: int = 0
        self._last_pit_window_alert_laps: int = 999
        self._coaching_suppressed_until: float = 0.0
        self._last_checkin_time: float = 0.0
        self._track_history: dict = {}
        self._prev_session_type: str = ''
        self._session_debrief_triggered: bool = False
        self._lap_sector_deltas: dict = {}
        self._per_lap_dynamics: dict = {}
        self._last_weather_alert: float = 0.0
        self._last_overcut_alert: float = 0.0
        self._last_weather_declared_wet: bool = False
        self._last_track_wetness: int = 0
        self._last_track_temp_alerted_c: float | None = None
        self._gap_history: dict = {'ahead': [], 'behind': []}
        self._last_blue_flag_alert: float = 0.0
        self._prev_blue_flag: bool = False
        self._prev_meatball: bool = False
        self._pit_stop_log: list = []
        self._prev_position: int | None = None
        self._prev_incidents: int = 0
        self._last_incident_alert: float = 0.0
        self._alert_gen: int = 0
        self._vad_thread: threading.Thread | None = None
        # Pit wall broadcast state
        self._tele_frame: dict = {}
        self._tele_lap_buf: list = []   # frames accumulated for the current lap
        self._tele_best_lap_s: float = 0.0
        self._broadcast_thread: threading.Thread | None = None
        self._last_gap_alert: float = 0.0
        self._last_fuel_diverge_alert: float = 0.0
        self._last_position_alert: float = 0.0
        self._last_driver_swap_alert: float = 0.0
        self._muted: bool = False

        # ── Style ────────────────────────────────────────────────────────
        style = ttk.Style(self)
        style.theme_use('clam')
        style.configure('TLabel',      background=BG,  foreground=TEXT, font=('Segoe UI', 9))
        style.configure('TFrame',      background=BG)
        style.configure('TLabelframe', background=BG2, foreground=TEXT, relief='flat')
        style.configure('TLabelframe.Label', background=BG2, foreground=TEXT,
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
        # Voice: store the display label, resolve to voice ID when saving
        saved_voice = self._cfg.get('tts_voice', DEFAULT_VOICE)
        voice_label = next((k for k, v in EDGE_VOICES.items() if v == saved_voice),
                           list(EDGE_VOICES.keys())[0])
        self.v_voice = tk.StringVar(value=voice_label)
        saved_personality = self._cfg.get('personality', DEFAULT_PERSONALITY)
        personality_label = next(
            (k for k, v in PERSONALITIES.items() if v == saved_personality),
            list(PERSONALITIES.keys())[0],
        )
        self.v_personality = tk.StringVar(value=personality_label)
        self.v_volume       = tk.DoubleVar(value=self._cfg.get('tts_volume', 1.0))
        self.v_units        = tk.StringVar(value=self._cfg.get('units_system', 'metric'))
        _ci_raw = self._cfg.get('checkin_laps', 5)
        self.v_checkin_laps = tk.StringVar(value='never' if not _ci_raw else str(_ci_raw))
        _cm_raw = self._cfg.get('checkin_mins', 0)
        self.v_checkin_mins = tk.StringVar(value='off' if not _cm_raw else str(_cm_raw))
        self.v_tts_rate = tk.DoubleVar(value=self._cfg.get('tts_rate', 1.0))
        self.v_listen_mode = tk.StringVar(value=self._cfg.get('listen_mode', 'ptt'))
        self.v_vad_sensitivity = tk.DoubleVar(
            value=self._cfg.get('vad_sensitivity', DEFAULTS['vad_sensitivity']))
        self.v_wake_word = tk.StringVar(value=self._cfg.get('wake_word', DEFAULTS['wake_word']))

        # ── TTS queue (single engine, no double-speak) ────────────────────
        self._tts_queue  = queue.Queue(maxsize=5)
        self._tts_thread = threading.Thread(target=self._tts_worker, daemon=True)
        self._tts_thread.start()

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
        self._hdr_canvas = tk.Canvas(self, bg=BG, highlightthickness=0, height=68)
        self._hdr_canvas.pack(fill='x', pady=(10, 4))
        self._hdr_canvas.bind('<Configure>', lambda e: self._draw_header_circuits())
        # Title drawn on canvas so it sits above the circuit traces
        self._hdr_title  = self._hdr_canvas.create_text(
            54, 30, text='NEURAL RACING PERFORMANCE',
            fill=TEXT, font=('Segoe UI', 11, 'bold'), anchor='w')
        self._hdr_sub    = self._hdr_canvas.create_text(
            54, 48, text='AI Race Engineer',
            fill=DIM, font=('Segoe UI', 8), anchor='w')
        self._hdr_icon   = self._hdr_canvas.create_text(
            20, 34, text='◈', fill=CYAN, font=('Segoe UI', 22), anchor='w')

    def _draw_header_circuits(self):
        c = self._hdr_canvas
        c.delete('circuit')
        w = c.winfo_width()
        h = c.winfo_height()
        if w < 10:
            return

        TRACE  = '#0c2e3a'   # dark trace line
        TRACE2 = '#164858'   # slightly brighter trace
        NODE   = CYAN        # bright via
        PAD    = '#0f3040'   # dim square pad

        # Three horizontal bus lines
        buses = [int(h * f) for f in (0.18, 0.52, 0.84)]
        for y in buses:
            c.create_line(0, y, w, y, fill=TRACE, width=1, tags='circuit')

        # Vertical traces at regular intervals, only on right 40% to avoid title text
        step = max(22, w // 20)
        for xi in range(0, w, step):
            if xi < w * 0.55:
                continue   # leave room for title text on left
            x = xi
            # Vary which buses get connected
            bucket = (x // step) % 3
            if bucket == 0:
                c.create_line(x, buses[0], x, buses[1], fill=TRACE, width=1, tags='circuit')
            elif bucket == 1:
                c.create_line(x, buses[1], x, buses[2], fill=TRACE, width=1, tags='circuit')
            else:
                c.create_line(x, buses[0], x, buses[2], fill=TRACE2, width=1, tags='circuit')

            # Draw pads / vias at intersections
            for idx, y in enumerate(buses):
                kind = (x // step + idx) % 4
                if kind == 0:
                    c.create_rectangle(x-2, y-2, x+2, y+2, fill=PAD, outline=TRACE2, tags='circuit')
                elif kind == 1:
                    c.create_oval(x-3, y-3, x+3, y+3, fill=NODE, outline='', tags='circuit')

        # Small IC chip rectangle on the far right
        if w > 200:
            ix = w - 28
            c.create_rectangle(ix-10, 10, ix+10, h-10, fill=PAD, outline=TRACE2, width=1, tags='circuit')
            for pf in (0.30, 0.52, 0.74):
                py = int(h * pf)
                c.create_line(ix-10, py, ix-16, py, fill=TRACE2, width=1, tags='circuit')
                c.create_line(ix+10, py, ix+16, py, fill=TRACE2, width=1, tags='circuit')

        # Raise title text above circuits
        c.tag_raise('title')  # re-raise title items — they have no tag, use specific IDs
        c.lift(self._hdr_title)
        c.lift(self._hdr_sub)
        c.lift(self._hdr_icon)

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
        cb.bind('<<ComboboxSelected>>', lambda _: self._save_fuel_unit_pref())

        # Row 5: Personality
        ttk.Label(frm, text='Personality').grid(row=5, column=0, sticky='w', pady=3, padx=(0, 10))
        personality_cb = ttk.Combobox(frm, textvariable=self.v_personality,
                                       values=list(PERSONALITIES.keys()),
                                       state='readonly', width=22)
        personality_cb.grid(row=5, column=1, sticky='w', pady=3)
        personality_cb.bind('<<ComboboxSelected>>', lambda _: self._save_personality_pref())

        # Row 6: Voice
        ttk.Label(frm, text='Voice').grid(row=6, column=0, sticky='w', pady=3, padx=(0, 10))
        voice_cb = ttk.Combobox(frm, textvariable=self.v_voice,
                                values=list(EDGE_VOICES.keys()),
                                state='readonly', width=32)
        voice_cb.grid(row=6, column=1, sticky='ew', pady=3, columnspan=2)
        voice_cb.bind('<<ComboboxSelected>>', lambda _: self._save_voice_pref())

        # Row 7: Spotter
        ttk.Label(frm, text='Spotter').grid(row=7, column=0, sticky='w', pady=3, padx=(0, 10))
        tk.Checkbutton(
            frm, text='Enable spotter callouts',
            variable=self.v_spotter,
            bg=BG2, fg=TEXT, selectcolor=BG3,
            activebackground=BG2, activeforeground=TEXT,
            font=('Segoe UI', 9),
            command=self._save_spotter_pref,
        ).grid(row=7, column=1, sticky='w', pady=3, columnspan=2)

        # Row 8: Volume
        ttk.Label(frm, text='Volume').grid(row=8, column=0, sticky='w', pady=3, padx=(0, 10))
        vol_frame = ttk.Frame(frm)
        vol_frame.grid(row=8, column=1, sticky='ew', pady=3, columnspan=2)
        tk.Scale(
            vol_frame, variable=self.v_volume,
            from_=0.0, to=1.0, resolution=0.05, orient='horizontal',
            bg=BG2, fg=TEXT, troughcolor=BG3, highlightthickness=0,
            activebackground=ACCENT, length=180, showvalue=False,
            command=lambda _: self._save_volume_pref(),
        ).pack(side='left')
        self._vol_pct_label = tk.Label(vol_frame, text=f'{int(self.v_volume.get()*100)}%',
                                       bg=BG2, fg=TEXT, font=('Segoe UI', 9), width=4)
        self._vol_pct_label.pack(side='left', padx=(6, 0))

        # Row 9: Units
        ttk.Label(frm, text='Units').grid(row=9, column=0, sticky='w', pady=3, padx=(0, 10))
        units_cb = ttk.Combobox(frm, textvariable=self.v_units,
                                values=['metric', 'imperial'],
                                state='readonly', width=12)
        units_cb.grid(row=9, column=1, sticky='w', pady=3)
        units_cb.bind('<<ComboboxSelected>>', lambda _: self._save_units_pref())

        # Row 10: Coaching check-in frequency (lap-based)
        ttk.Label(frm, text='Check-in every').grid(row=10, column=0, sticky='w', pady=3, padx=(0, 10))
        checkin_cb = ttk.Combobox(frm, textvariable=self.v_checkin_laps,
                                  values=['3', '5', '10', 'never'],
                                  state='readonly', width=12)
        checkin_cb.grid(row=10, column=1, sticky='w', pady=3)
        ttk.Label(frm, text='laps  or').grid(row=10, column=2, sticky='w', pady=3)
        checkin_cb.bind('<<ComboboxSelected>>', lambda _: self._save_checkin_pref())
        checkin_mins_cb = ttk.Combobox(frm, textvariable=self.v_checkin_mins,
                                       values=['5', '10', '15', '20', '30', 'off'],
                                       state='readonly', width=8)
        checkin_mins_cb.grid(row=10, column=3, sticky='w', pady=3, padx=(4, 0))
        ttk.Label(frm, text='min').grid(row=10, column=4, sticky='w', pady=3)
        checkin_mins_cb.bind('<<ComboboxSelected>>', lambda _: self._save_checkin_mins_pref())

        # Row 11: Voice Speed
        ttk.Label(frm, text='Voice Speed').grid(row=11, column=0, sticky='w', pady=3, padx=(0, 10))
        rate_frame = ttk.Frame(frm)
        rate_frame.grid(row=11, column=1, sticky='ew', pady=3, columnspan=2)
        tk.Scale(
            rate_frame, variable=self.v_tts_rate,
            from_=0.5, to=2.0, resolution=0.1, orient='horizontal',
            bg=BG2, fg=TEXT, troughcolor=BG3, highlightthickness=0,
            activebackground=ACCENT, length=180, showvalue=False,
            command=lambda _: self._save_rate_pref(),
        ).pack(side='left')
        self._rate_label = tk.Label(rate_frame,
                                    text=f'{self.v_tts_rate.get():.1f}x',
                                    bg=BG2, fg=TEXT, font=('Segoe UI', 9), width=4)
        self._rate_label.pack(side='left', padx=(6, 0))

        # Row 12: Listen Mode (PTT / VAD / Wake Word)
        ttk.Label(frm, text='Listen Mode').grid(row=12, column=0, sticky='w', pady=3, padx=(0, 10))
        listen_cb = ttk.Combobox(frm, textvariable=self.v_listen_mode,
                                 values=['ptt', 'vad', 'wake'], state='readonly', width=12)
        listen_cb.grid(row=12, column=1, sticky='w', pady=3)
        listen_cb.bind('<<ComboboxSelected>>', lambda _: self._save_listen_mode_pref())
        tk.Label(frm, text='ptt = hold  |  vad = always-on  |  wake = wake word',
                 bg=BG2, fg=DIM, font=('Segoe UI', 8),
                 ).grid(row=12, column=2, sticky='w', pady=3, padx=(6, 0))

        # Row 12b: Wake word entry (only shown when mode = wake)
        self._wake_word_label = ttk.Label(frm, text='Wake Word')
        self._wake_word_entry = ttk.Entry(frm, textvariable=self.v_wake_word, width=20)
        self._wake_word_entry.bind('<FocusOut>', lambda _: self._save_wake_word_pref())
        self._wake_word_entry.bind('<Return>',   lambda _: self._save_wake_word_pref())
        self._wake_word_label.grid(row=12, column=3, sticky='w', pady=3, padx=(16, 4))
        self._wake_word_entry.grid(row=12, column=4, sticky='w', pady=3)
        # Show/hide based on current mode
        self._update_wake_word_visibility()
        listen_cb.bind('<<ComboboxSelected>>',
                       lambda _: (self._save_listen_mode_pref(),
                                  self._update_wake_word_visibility()))

        # Row 13: VAD Sensitivity (only meaningful in vad/wake mode)
        ttk.Label(frm, text='VAD Sensitivity').grid(row=13, column=0, sticky='w', pady=3, padx=(0, 10))
        vad_frame = ttk.Frame(frm)
        vad_frame.grid(row=13, column=1, sticky='ew', pady=3, columnspan=2)
        tk.Scale(
            vad_frame, variable=self.v_vad_sensitivity,
            from_=0.005, to=0.10, resolution=0.005, orient='horizontal',
            bg=BG2, fg=TEXT, troughcolor=BG3, highlightthickness=0,
            activebackground=ACCENT, length=180, showvalue=False,
            command=lambda _: self._save_vad_sensitivity_pref(),
        ).pack(side='left')
        self._vad_sens_label = tk.Label(vad_frame,
                                        text=f'{self.v_vad_sensitivity.get():.3f}',
                                        bg=BG2, fg=TEXT, font=('Segoe UI', 9), width=5)
        self._vad_sens_label.pack(side='left', padx=(6, 0))

    def _save_fuel_unit_pref(self):
        self._cfg['fuel_unit'] = self.v_fuel_unit.get()
        save_config(self._cfg)

    def _save_spotter_pref(self):
        self._cfg['spotter_enabled'] = self.v_spotter.get()
        save_config(self._cfg)

    def _save_voice_pref(self):
        label = self.v_voice.get()
        self._cfg['tts_voice'] = EDGE_VOICES.get(label, DEFAULT_VOICE)
        save_config(self._cfg)

    def _save_personality_pref(self):
        label = self.v_personality.get()
        self._cfg['personality'] = PERSONALITIES.get(label, DEFAULT_PERSONALITY)
        save_config(self._cfg)

    def _save_volume_pref(self):
        vol = self.v_volume.get()
        self._cfg['tts_volume'] = vol
        self._vol_pct_label.config(text=f'{int(vol * 100)}%')
        save_config(self._cfg)

    def _save_units_pref(self):
        self._cfg['units_system'] = self.v_units.get()
        save_config(self._cfg)

    def _save_checkin_pref(self):
        val = self.v_checkin_laps.get()
        self._cfg['checkin_laps'] = 0 if val == 'never' else int(val)
        save_config(self._cfg)

    def _save_checkin_mins_pref(self):
        val = self.v_checkin_mins.get()
        self._cfg['checkin_mins'] = 0 if val == 'off' else int(val)
        save_config(self._cfg)

    def _save_rate_pref(self):
        rate = self.v_tts_rate.get()
        self._cfg['tts_rate'] = rate
        self._rate_label.config(text=f'{rate:.1f}x')
        save_config(self._cfg)

    def _save_listen_mode_pref(self):
        self._cfg['listen_mode'] = self.v_listen_mode.get()
        save_config(self._cfg)

    def _save_wake_word_pref(self):
        self._cfg['wake_word'] = self.v_wake_word.get().strip().lower() or DEFAULTS['wake_word']
        save_config(self._cfg)

    def _update_wake_word_visibility(self):
        if self.v_listen_mode.get() == 'wake':
            self._wake_word_label.grid()
            self._wake_word_entry.grid()
        else:
            self._wake_word_label.grid_remove()
            self._wake_word_entry.grid_remove()

    def _save_vad_sensitivity_pref(self):
        val = self.v_vad_sensitivity.get()
        self._cfg['vad_sensitivity'] = val
        self._vad_sens_label.config(text=f'{val:.3f}')
        save_config(self._cfg)

    def _update_pitwall_btn(self):
        if self._server_session_id:
            self.pitwall_btn.pack(side='left')
            self.v_pitwall_status.set(f'Session {self._server_session_id} — share with teammates')

    def _copy_pitwall_link(self):
        if not self._server_session_id:
            return
        url = f'{BACKEND_URL}/engineer/pitwall/{self._server_session_id}'
        self.clipboard_clear()
        self.clipboard_append(url)
        self.v_pitwall_status.set('Link copied!')
        self.after(3000, lambda: self.v_pitwall_status.set(
            f'Session {self._server_session_id} — share with teammates'))

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
        tk.Label(dlg, text='The app will close, update, and relaunch automatically.',
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
                    # Download into the same folder as the running EXE so Defender
                    # doesn't flag it as a suspicious file arriving in %TEMP%
                    if getattr(sys, 'frozen', False):
                        dl_dir = os.path.dirname(sys.executable)
                    else:
                        dl_dir = tempfile.gettempdir()
                    tmp_path = os.path.join(dl_dir, f'_nrp_update_{os.getpid()}.exe')
                    with requests.get(exe_url, stream=True, timeout=120) as resp:
                        resp.raise_for_status()
                        total = int(resp.headers.get('content-length', 0))
                        done  = 0
                        with open(tmp_path, 'wb') as f:
                            for chunk in resp.iter_content(chunk_size=65536):
                                if chunk:
                                    f.write(chunk)
                                    done += len(chunk)
                                    if total:
                                        pct = done / total * 100
                                        self.after(0, lambda p=pct: progress_var.set(p))
                    self.after(0, lambda: status_var.set('Installing…'))
                    self.after(500, lambda: _apply_update(tmp_path))
                except Exception as e:
                    self.after(0, lambda err=e: (
                        status_var.set(f'Download failed: {err}'),
                        install_btn.config(state='normal'),
                    ))

            threading.Thread(target=download, daemon=True).start()

        install_btn.config(command=do_install)

    def _toggle_mute(self):
        self._muted = not self._muted
        if self._muted:
            while not self._tts_queue.empty():
                try:
                    self._tts_queue.get_nowait()
                except queue.Empty:
                    break
        self.mute_btn.config(text='Unmute' if self._muted else 'Mute')
        self.log('[MUTE] Voice muted.' if self._muted else '[MUTE] Voice unmuted.')

    SNOOZE_SECONDS = 300  # 5-minute advisory snooze

    def _acknowledge(self):
        """Drain TTS queue and snooze all advisory coaching for SNOOZE_SECONDS."""
        while not self._tts_queue.empty():
            try:
                self._tts_queue.get_nowait()
            except queue.Empty:
                break
        self._coaching_suppressed_until = time.time() + self.SNOOZE_SECONDS
        self.speak("Copy that")
        self.log(f'[ACK] Acknowledged — advisory coaching snoozed {self.SNOOZE_SECONDS // 60} min')
        self.after(0, self._reset_talk_label)
        self.after(0, self._update_ack_label)

    def _update_ack_label(self):
        """Refresh the snooze badge text; reschedules itself every 30s while active."""
        remaining = self._coaching_suppressed_until - time.time()
        if remaining > 0:
            mins = int(remaining // 60) + 1
            self._ack_label.config(
                text=f'SNOOZED {mins}m', fg=YELLOW, bg=BG)
            self.after(30000, self._update_ack_label)
        else:
            self._ack_label.config(text='', fg=BG, bg=BG)

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
            # Start server session now that we know the track
            self._start_server_session(plan.get('track', plan.get('name', '')),
                                       plan.get('car', ''))
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
        except Exception as e:
            self.log(f'[AUTO] Stint recalculation failed after FPL update: {e}')

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
        self.mute_btn = ttk.Button(bf, text='Mute', style='Browse.TButton',
                                   command=self._toggle_mute)
        self.mute_btn.pack(side='left', padx=(8, 0))
        self.ack_btn = ttk.Button(bf, text='✓ Copy', style='Browse.TButton',
                                  command=self._acknowledge)
        self.ack_btn.pack(side='left', padx=(8, 0))
        self._ack_label = tk.Label(bf, text='', bg=BG, fg=BG,
                                   font=('Segoe UI', 8, 'bold'))
        self._ack_label.pack(side='left', padx=(4, 0))
        tk.Label(bf, text='iRacing must be running before starting.',
                 bg=BG, fg=DIM, font=('Segoe UI', 8)).pack(side='right')

    def _build_stint_panel(self):
        pf = ttk.LabelFrame(self, text='LIVE RACE STATE', padding=10)
        pf.pack(fill='x', padx=14, pady=4)

        self._stint_vars = {
            'driver':   tk.StringVar(value='—'),
            'lap':      tk.StringVar(value='—'),
            'fuel':     tk.StringVar(value='—'),
            'pit':      tk.StringVar(value='—'),
            'pos':      tk.StringVar(value='—'),
            'pit_time': tk.StringVar(value='—'),
            'last_lap': tk.StringVar(value='—'),
        }
        labels = [
            ('DRIVER',   'driver',   0, 0),
            ('LAP',      'lap',      0, 2),
            ('FUEL %',   'fuel',     1, 0),
            ('TO PIT',   'pit',      1, 2),
            ('POS',      'pos',      2, 0),
            ('PIT IN',   'pit_time', 2, 2),
            ('LAST LAP', 'last_lap', 3, 0),
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
        self._waiting_label.grid(row=8, column=0, columnspan=4, pady=(4, 0))

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

        # Pit wall share button (hidden until session starts)
        pw_frame = ttk.Frame(self)
        pw_frame.pack(fill='x', padx=14, pady=(0, 2))
        self.pitwall_btn = ttk.Button(
            pw_frame, text='Copy Pit Wall Link',
            style='Browse.TButton', command=self._copy_pitwall_link,
        )
        self.pitwall_btn.pack(side='left')
        self.v_pitwall_status = tk.StringVar(value='')
        tk.Label(pw_frame, textvariable=self.v_pitwall_status,
                 bg=BG2, fg=DIM, font=('Segoe UI', 8)).pack(side='left', padx=(8, 0))
        self.pitwall_btn.pack_forget()  # hidden until session_id is set

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

        # Load plan immediately (local disk — no network needed)
        if os.path.exists(PLAN_PATH):
            try:
                with open(PLAN_PATH) as f:
                    plan = json.load(f)
                self._plan   = plan
                self._stints = _calculate_stints(plan)
                self._update_plan_display()
            except Exception as e:
                self.log(f'Could not load race plan: {e}')
                self._plan   = {}
                self._stints = []
        else:
            self._show_wizard(start_at_plan=True)

        # Token validation runs in a background thread so the UI is never frozen
        def _validate():
            try:
                r = requests.post(
                    f'{BACKEND_URL}/engineer/validate',
                    json={'token': token},
                    timeout=8,
                )
                if r.status_code == 401:
                    self.after(0, lambda: (
                        self.log('Session expired — please log in again.'),
                        self._cfg.update({'token': ''}),
                        save_config(self._cfg),
                        self._show_wizard(),
                    ))
                    return
                elif r.ok:
                    resp  = r.json()
                    dname = resp.get('display_name', self._cfg.get('display_name', ''))
                    qt    = resp.get('queries_today', 0)
                    ql    = resp.get('query_limit', 50)
                    rem   = ql - qt
                    self._display_name  = dname
                    self._queries_today = qt
                    self._query_limit   = ql
                    self._cfg['display_name'] = dname
                    save_config(self._cfg)
                    self.after(0, lambda: (
                        self.v_acct_label.set(dname or 'Logged in'),
                        self.v_queries.set(
                            f'{qt} / {ql} today  ({rem} remaining)'
                        ),
                    ))
                else:
                    self.after(0, lambda: self.v_acct_label.set(
                        self._cfg.get('display_name', 'Logged in (offline)')))
            except Exception:
                self.after(0, lambda: self.v_acct_label.set(
                    self._cfg.get('display_name', 'Logged in (offline)')))
            self._load_history_from_server()

        threading.Thread(target=_validate, daemon=True).start()

    # ── First-run wizard ─────────────────────────────────────────────────────

    def _show_wizard(self, start_at_plan: bool = False):
        """Modal wizard: account login/register → race plan setup."""
        dlg = tk.Toplevel(self)
        dlg.title('Neural Racing Performance — Setup')
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
            dlg.unbind('<Return>')
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
            label(container, 'Welcome to Neural Racing Performance', size=14, bold=True, pady=(0, 4))
            label(container, 'AI Race Engineer', size=9, color=DIM, pady=(0, 20))
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

            dlg.bind('<Return>', lambda e: do_register())
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

            dlg.bind('<Return>', lambda e: do_login())
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
            v_lap_time   = field('Lap Time Target (seconds)', '120.0')
            v_capacity   = field('Fuel Capacity (litres)', '50.0')
            v_fpl        = field('Fuel Per Lap (litres)', '2.5')
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

        # Championship context section
        tk.Label(scroll_frm, text='Championship Context (optional)', bg=BG, fg=DIM,
                 font=('Segoe UI', 9, 'bold'), anchor='w').pack(fill='x', pady=(10, 2))
        existing_champ = plan.get('championship_context', {})
        v_champ_enabled = tk.BooleanVar(value=bool(existing_champ.get('enabled', False)))
        champ_toggle_row = tk.Frame(scroll_frm, bg=BG)
        champ_toggle_row.pack(fill='x')
        tk.Checkbutton(champ_toggle_row, text='Enable championship mode',
                       variable=v_champ_enabled, bg=BG, fg=TEXT, selectcolor=BG3,
                       activebackground=BG, activeforeground=TEXT,
                       font=('Segoe UI', 9)).pack(anchor='w')
        champ_detail = tk.Frame(scroll_frm, bg=BG)
        champ_detail.pack(fill='x', padx=10)

        def cfield(lbl, default):
            tk.Label(champ_detail, text=lbl, bg=BG, fg=DIM,
                     font=('Segoe UI', 8), anchor='w').pack(fill='x', pady=(3, 0))
            var = tk.StringVar(value=str(default))
            ttk.Entry(champ_detail, textvariable=var).pack(fill='x')
            return var

        v_champ_name  = cfield('Championship Name',          existing_champ.get('championship_name', ''))
        v_my_pts      = cfield('Your Current Points',         existing_champ.get('current_points', 0))
        v_lead_pts    = cfield('Leader Points',               existing_champ.get('points_leader_points', 0))
        v_lead_name   = cfield('Leader Name',                 existing_champ.get('points_leader_name', ''))
        v_race_num    = cfield('Race Number',                  existing_champ.get('race_number', 1))
        v_races_rem   = cfield('Races Remaining',              existing_champ.get('races_remaining', 10))
        _pts_default  = ','.join(map(str, existing_champ.get('points_per_position',
                                                              [25, 18, 15, 12, 10, 8, 6, 4, 2, 1])))
        v_pts_table   = cfield('Points Per Position (comma-separated)', _pts_default)

        def _toggle_champ(*_):
            state = 'normal' if v_champ_enabled.get() else 'disabled'
            for w in champ_detail.winfo_children():
                try: w.config(state=state)
                except Exception: pass
        v_champ_enabled.trace_add('write', _toggle_champ)
        _toggle_champ()

        v_err = tk.StringVar()
        tk.Label(outer, textvariable=v_err, bg=BG, fg=ACCENT,
                 font=('Segoe UI', 8), wraplength=420).pack(pady=2)

        def _parse_pts_table(s):
            try:
                return [int(x.strip()) for x in s.split(',') if x.strip().lstrip('-').isdigit()]
            except Exception:
                return [25, 18, 15, 12, 10, 8, 6, 4, 2, 1]

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
                    'championship_context': {
                        'enabled':              v_champ_enabled.get(),
                        'championship_name':    v_champ_name.get().strip(),
                        'current_points':       int(v_my_pts.get()   or 0),
                        'points_leader_points': int(v_lead_pts.get() or 0),
                        'points_leader_name':   v_lead_name.get().strip(),
                        'race_number':          int(v_race_num.get() or 1),
                        'races_remaining':      int(v_races_rem.get() or 10),
                        'points_per_position':  _parse_pts_table(v_pts_table.get()),
                    },
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

    # ── Session memory ────────────────────────────────────────────────────────

    def _load_session_memory(self):
        try:
            files = [
                os.path.join(SESSIONS_DIR, f)
                for f in os.listdir(SESSIONS_DIR)
                if f.endswith('.json')
            ]
            files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
            recent = files[:3]
            if not recent:
                return
            lines = ['PAST SESSIONS (most recent first):']
            for fp in recent:
                try:
                    with open(fp) as f:
                        s = json.load(f)
                    date   = s.get('date', '?')[:10]
                    track  = s.get('track', '?')
                    best   = s.get('best_lap', '?')
                    fpl    = s.get('avg_fpl', '?')
                    stints = s.get('stints_completed', '?')
                    notes  = s.get('notes', [])
                    note_str = '; '.join(notes[-2:]) if notes else ''
                    lines.append(
                        f"- {date} | {track} | Best lap: {best} | "
                        f"Avg FPL: {fpl}L | {stints} stints"
                        + (f' | Notes: {note_str}' if note_str else '')
                    )
                except Exception:
                    pass
            self._session_memory_summary = '\n'.join(lines)
        except Exception:
            pass

    def _save_session_memory(self):
        try:
            plan_name  = self._plan.get('name', 'unknown') if self._plan else 'unknown'
            # Use the dedicated 'track' field when available; fall back to splitting the name
            track      = (self._plan.get('track') or plan_name.split(' · ')[0]) if self._plan else 'unknown'
            track_slug = ''.join(c if c.isalnum() else '_' for c in track)[:40]
            date_str = time.strftime('%Y-%m-%d')
            best = f'{self._session_best_lap:.3f}s' if self._session_best_lap > 0 else '?'
            fd = {}
            with self._ctx_lock:
                ctx = self._ctx
            if ctx:
                fd = ctx.get('telemetry', {}).get('fuel_delta', {})
            avg_fpl = round(fd.get('avg_actual_fpl', 0), 3) if fd.get('avg_actual_fpl') else '?'
            stints_done = 0
            if ctx:
                live = ctx.get('live', {})
                cs   = live.get('current_stint', {})
                stints_done = cs.get('stint_num', 0) or 0
            data = {
                'date':             date_str,
                'track':            track,
                'best_lap':         best,
                'avg_fpl':          avg_fpl,
                'stints_completed': stints_done,
                'notes':            self._session_notes[-5:],
            }
            fname = f'{date_str}_{track_slug}.json'
            with open(os.path.join(SESSIONS_DIR, fname), 'w') as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    def _start_server_session(self, track_name: str, car_name: str):
        """Register a new session with the backend DB. Fires once per iRacing connect."""
        if self._session_started:
            return
        self._session_started = True
        token = self._cfg.get('token', '')
        if not token:
            return
        plan = self._plan or {}
        def _do():
            try:
                r = requests.post(
                    f'{BACKEND_URL}/engineer/session/start',
                    json={
                        'token':             token,
                        'track_name':        track_name,
                        'car_name':          car_name,
                        'race_duration_hrs': plan.get('race_duration_hrs', 0),
                    },
                    timeout=8,
                )
                if r.ok:
                    self._server_session_id = r.json().get('session_id')
                    self.log(f'[HISTORY] Session {self._server_session_id} started.')
                    self.after(0, self._start_broadcast)
                    self.after(0, self._update_pitwall_btn)
            except Exception:
                pass
        threading.Thread(target=_do, daemon=True).start()
        # Fetch track/car history in parallel so AI has context from first lap
        if track_name:
            threading.Thread(
                target=self._load_track_history,
                args=(track_name, car_name),
                daemon=True,
            ).start()

    def _load_track_history(self, track_name: str, car_name: str):
        """Fetch aggregate stats for this track+car and store for system prompt injection."""
        token = self._cfg.get('token', '')
        if not token:
            return
        try:
            r = requests.get(
                f'{BACKEND_URL}/engineer/track-stats',
                params={'token': token, 'track': track_name, 'car': car_name},
                timeout=8,
            )
            if not r.ok:
                return
            data = r.json()
            if not data.get('found') or not data.get('session_count'):
                self.log(f'[HISTORY] No previous sessions at {track_name}.')
                return
            n       = data['session_count']
            best    = data.get('best_lap_s')
            avg_fpl = data.get('avg_fpl_l')
            laps    = data.get('total_laps', 0)
            last    = data.get('last_session_date', '')
            self._track_history = data
            parts = [f"{n} previous session{'s' if n != 1 else ''} at this track/car"]
            if best:
                m, s = divmod(best, 60)
                parts.append(f"personal best {int(m)}:{s:06.3f}")
            if avg_fpl:
                parts.append(f"typical fuel burn {avg_fpl:.3f}L/lap")
            if laps:
                parts.append(f"{laps} laps logged")
            if last:
                parts.append(f"last raced {last}")
            summary = f"YOUR HISTORY HERE: {', '.join(parts)}."
            self._session_memory_summary = (
                (self._session_memory_summary + '\n' if self._session_memory_summary else '')
                + summary
            )
            self.log(f'[HISTORY] Track history loaded: {summary}')
        except Exception as e:
            self.log(f'[HISTORY] Track stats error: {e}')

    def _record_server_lap(self, lap_num: int, lap_time_s: float,
                            fuel_used_l: float | None, position: int | None):
        """Send a completed lap to the backend DB. Fire-and-forget."""
        if not self._server_session_id:
            return
        token = self._cfg.get('token', '')
        if not token:
            return
        def _do():
            try:
                requests.post(
                    f'{BACKEND_URL}/engineer/session/lap',
                    json={
                        'token':       token,
                        'session_id':  self._server_session_id,
                        'lap_num':     lap_num,
                        'lap_time_s':  lap_time_s,
                        'fuel_used_l': fuel_used_l,
                        'position':    position,
                    },
                    timeout=6,
                )
            except Exception:
                pass
        threading.Thread(target=_do, daemon=True).start()

    def _end_server_session(self):
        """Finalise the session record in the backend DB."""
        if not self._server_session_id:
            return
        token = self._cfg.get('token', '')
        if not token:
            return
        sid = self._server_session_id
        self._server_session_id = None
        self._session_started   = False
        def _do():
            try:
                requests.post(
                    f'{BACKEND_URL}/engineer/session/end',
                    json={
                        'token':        token,
                        'session_id':   sid,
                        'total_stints': len(self._stints),
                    },
                    timeout=8,
                )
            except Exception:
                pass
        threading.Thread(target=_do, daemon=True).start()

    def _do_session_debrief(self):
        """Generate an AI post-session debrief. Called when engineer is stopped after 5+ laps."""
        if self._session_debrief_triggered:
            return
        if len(self._lap_times_this_session) < 5:
            return
        token = self._cfg.get('token', '')
        if not token:
            return
        self._session_debrief_triggered = True

        times       = self._lap_times_this_session  # rolling last-10 window for pace analysis
        total_laps  = self._total_laps_this_session  # true lap count regardless of window
        n           = len(times)
        best        = self._session_best_lap
        avg         = sum(times) / n
        f5          = sum(times[:5]) / min(5, n)
        l5          = sum(times[-5:]) / min(5, n)
        trend       = l5 - f5
        trend_word  = 'improving' if trend < -0.2 else ('degrading' if trend > 0.2 else 'consistent')

        with self._ctx_lock:
            ctx = self._ctx
        incidents   = ctx.get('telemetry', {}).get('incidents', 0) if ctx else 0
        stints_done = 0
        if ctx:
            stints_done = (ctx.get('live', {}).get('current_stint', {}) or {}).get('stint_num', 0) or 0
        avg_fpl_str = '?'
        if ctx:
            fd = ctx.get('telemetry', {}).get('fuel_delta', {})
            if fd.get('avg_actual_fpl'):
                avg_fpl_str = f"{fd['avg_actual_fpl']:.3f}L/lap"

        # Sector averages across all laps with recorded data
        sector_summary_str = ''
        if self._lap_sector_deltas:
            s_avgs = {}
            for sx in ('s1', 's2', 's3'):
                vals = [v[sx] for v in self._lap_sector_deltas.values() if sx in v]
                if vals:
                    s_avgs[sx] = sum(vals) / len(vals)
            if s_avgs:
                parts = [f"{sx.upper()}: {avg_d:+.3f}s avg vs best" for sx, avg_d in s_avgs.items()]
                worst = max(s_avgs, key=lambda k: s_avgs[k])
                sector_summary_str = (
                    f" Session sector averages vs best: {', '.join(parts)}."
                    f" Weakest sector overall: {worst.upper()}."
                )

        # Handling summary across all laps (values are now % of lap, not raw frame counts)
        handling_summary_str = ''
        if self._per_lap_dynamics:
            vals_over  = [d.get('oversteer',  0) for d in self._per_lap_dynamics.values()]
            vals_under = [d.get('understeer', 0) for d in self._per_lap_dynamics.values()]
            n_laps_dyn = len(self._per_lap_dynamics)
            avg_over   = sum(vals_over)  / n_laps_dyn
            avg_under  = sum(vals_under) / n_laps_dyn
            if avg_over > avg_under * 1.5 and avg_over > 3:
                handling_summary_str = (
                    f" Handling: consistent oversteer tendency across the session "
                    f"({avg_over:.1f}% avg per lap over {n_laps_dyn} laps) — consider setup or brake bias adjustment."
                )
            elif avg_under > avg_over * 1.5 and avg_under > 3:
                handling_summary_str = (
                    f" Handling: consistent understeer tendency across the session "
                    f"({avg_under:.1f}% avg per lap over {n_laps_dyn} laps) — consider setup or brake bias adjustment."
                )

        # Consistency across the session
        consistency_summary_str = ''
        if len(times) >= 4:
            mean_t  = sum(times) / len(times)
            std_dev = (sum((t - mean_t) ** 2 for t in times) / len(times)) ** 0.5
            consistency_summary_str = f" Lap-to-lap consistency: ±{std_dev:.2f}s std dev."

        track_name   = (ctx.get('plan', {}).get('track', '') or '') if ctx else ''
        track_debrief_hint = (
            f" Sectors are approximate thirds of the lap at {track_name} — "
            f"name the relevant corners when discussing sector weaknesses."
        ) if (track_name and sector_summary_str) else ''

        system_prompt = self._build_system_prompt(ctx) if ctx else ''
        question = (
            f"Session complete — give a debrief. "
            f"{total_laps} laps total. Best: {self._fmt_lap_spoken(best)}. "
            f"Avg (last {n}): {self._fmt_lap_spoken(avg)}. "
            f"Pace trend: {trend_word} ({trend:+.2f}s first 5 vs last 5 of sample)."
            f"{sector_summary_str}{handling_summary_str}{consistency_summary_str}{track_debrief_hint} "
            f"Incidents: {incidents}. Stints: {stints_done}. Avg fuel: {avg_fpl_str}. "
            f"Give 3-4 sentences: one on overall pace, one on the weakest area to focus on "
            f"next time (use sector and handling data to be specific), one on any setup or "
            f"technique recommendation. Do not repeat the lap time."
        )
        self.log('[DEBRIEF] Generating session debrief…')

        def _do():
            try:
                r = requests.post(
                    f'{BACKEND_URL}/engineer/coaching',
                    json={'token': token, 'system_prompt': system_prompt, 'question': question},
                    timeout=15,
                )
                if r.ok:
                    answer = r.json().get('answer', '')
                    if answer:
                        self.log(f'[DEBRIEF] {answer}')
                        self.after(0, lambda: self._append_qa('[Session Debrief]', answer))
                        self.speak(answer)
            except Exception as e:
                self.log(f'[DEBRIEF] Error: {e}')

        threading.Thread(target=_do, daemon=True).start()

    def _on_opponent_pit_exit(self, opp_name: str, opp_position: int, pit_entry_lap: int):
        """Fired when a nearby opponent exits the pits. Alerts and triggers strategy coaching."""
        if not self._running:
            return
        with self._ctx_lock:
            ctx = self._ctx
        if not ctx:
            return
        if ctx.get('session', {}).get('type', '').lower() != 'race':
            return
        my_pos = ctx.get('telemetry', {}).get('opponents', {}).get('my_position')
        now = time.time()
        if now - self._last_overcut_alert < 90:
            return
        self._last_overcut_alert = now
        laps_til_pit = ctx.get('live', {}).get('laps_until_pit')
        if opp_position < (my_pos or 999):
            msg = (f"Undercut alert: {opp_name} in P{opp_position} has pitted. "
                   f"They'll rejoin on fresh tyres soon.")
        else:
            extra = (f" You have {laps_til_pit} laps to your window." if laps_til_pit else '')
            msg = f"Strategy: P{opp_position} {opp_name} has pitted.{extra}"
        self.log(f'[STRATEGY] {msg}')
        self._say('undercut_alert', msg, 90)
        if not self._coaching_in_flight:
            self._coaching_in_flight = True
            threading.Thread(target=self._ask_strategy_coaching,
                             args=(opp_name, opp_position, pit_entry_lap),
                             daemon=True).start()

    def _ask_strategy_coaching(self, opp_name: str, opp_position: int, pit_entry_lap: int):
        """Ask the AI for overcut/undercut strategy analysis. Fire-and-forget thread."""
        try:
            with self._ctx_lock:
                ctx = self._ctx
            system_prompt = self._build_system_prompt(ctx) if ctx else ''
            current_lap   = ctx.get('telemetry', {}).get('current_lap', 0) if ctx else 0
            question = (
                f"{opp_name} in P{opp_position} just pitted (entered on lap {pit_entry_lap}, "
                f"we are now on lap {current_lap}). Assess the overcut/undercut implications — "
                f"should we stay out, pit now, or adjust our window? 2-3 sentences."
            )
            token = self._cfg.get('token', '')
            if not token:
                return
            r = requests.post(
                f'{BACKEND_URL}/engineer/coaching',
                json={'token': token, 'system_prompt': system_prompt, 'question': question},
                timeout=12,
            )
            if r.ok:
                answer = r.json().get('answer', '')
                if answer:
                    self.log(f'[STRATEGY] {answer}')
                    self.after(0, lambda: self._append_qa(
                        f'[Strategy] {opp_name} P{opp_position} pitted', answer))
                    self.speak(answer)
        except Exception as e:
            self.log(f'Strategy coaching error: {e}')
        finally:
            self._coaching_in_flight = False

    def _ask_pit_strategy(self, trigger: str = 'driver_request'):
        """Ask the AI for a single pit-now-or-stay-out recommendation. Fire-and-forget."""
        if self._coaching_in_flight:
            return
        token = self._cfg.get('token', '')
        if not token:
            return
        with self._ctx_lock:
            ctx = self._ctx
        if not ctx:
            return

        live  = ctx.get('live', {})
        tele  = ctx.get('telemetry', {})
        plan  = ctx.get('plan', {})
        units = self._cfg.get('fuel_unit', 'gal')
        imp   = (units == 'gal')

        def _fmt(l):
            if l is None: return '?'
            return f"{l/3.78541:.2f}gal" if imp else f"{l:.1f}L"

        fuel_sensor    = tele.get('fuel_level')
        avg_fpl        = tele.get('fuel_delta', {}).get('avg_actual_fpl')
        fuel_measured  = tele.get('fuel_laps_measured', 0)
        sensor_laps    = round(fuel_sensor / avg_fpl, 1) if (fuel_sensor and avg_fpl) else None
        laps_until_pit = live.get('laps_until_pit', '?')
        pit_optimal    = live.get('pit_window_optimal', '?')
        pit_last       = live.get('pit_window_last', '?')
        pit_status     = live.get('pit_window_status', 'unknown').upper()
        opp            = tele.get('opponents', {})
        ahead          = opp.get('ahead')
        behind         = opp.get('behind')
        my_pos         = opp.get('my_position', '?')
        current_lap    = tele.get('current_lap', '?')

        gap_ahead_str  = f"{ahead['name']} {ahead['gap']:.1f}s ahead" if ahead else "clear ahead"
        gap_behind_str = f"{behind['name']} {behind['gap']:.1f}s behind" if behind else "clear behind"

        if sensor_laps is not None:
            fuel_str = f"{_fmt(fuel_sensor)} | {sensor_laps} laps remaining (measured over {fuel_measured} laps)"
        else:
            fuel_str = f"{_fmt(fuel_sensor)} | laps remaining unknown (still measuring fuel burn)"

        recent_stops = []
        for entry in self._pit_stop_log[-2:]:
            recent_stops.append(f"{entry['duration_s']:.1f}s stop ({'+'.join(entry['services']) or 'unknown services'})")
        stops_str = '; '.join(recent_stops) if recent_stops else 'none this session'

        situation = (
            f"LAP: {current_lap} | POSITION: P{my_pos}\n"
            f"GAP: {gap_ahead_str} | {gap_behind_str}\n"
            f"PIT WINDOW: optimal lap {pit_optimal}, last safe lap {pit_last}, "
            f"{laps_until_pit} laps away, status {pit_status}\n"
            f"FUEL: {fuel_str}\n"
            f"RECENT PIT STOPS THIS SESSION: {stops_str}"
        )
        question = (
            "Should I pit this lap, extend my stint, or is there no strategic advantage either way? "
            "Give one clear recommendation ('Pit this lap', 'Extend N more laps', or 'No change') "
            "and one sentence of reasoning. Maximum 2 sentences."
        )
        system = (
            "You are a professional endurance race engineer giving a real-time pit strategy call. "
            "The driver needs a single decisive recommendation right now — no hedging, no multiple options. "
            "Format: recommendation on one line, reasoning on the next. Max 2 sentences total."
        )

        self.log(f'[STRATEGY] Pit strategy requested ({trigger})')
        self._coaching_in_flight = True

        def _do():
            try:
                r = requests.post(
                    f'{BACKEND_URL}/engineer/coaching',
                    json={'token': token, 'system_prompt': system,
                          'question': f"{situation}\n\n{question}"},
                    timeout=12,
                )
                if r.ok:
                    answer = r.json().get('answer', '')
                    if answer:
                        self.log(f'[STRATEGY] {answer}')
                        self.after(0, lambda: self._append_qa('[Strategy] Pit call?', answer))
                        self.speak(answer)
            except Exception as e:
                self.log(f'Pit strategy error: {e}')
            finally:
                self._coaching_in_flight = False

        threading.Thread(target=_do, daemon=True).start()

    def _speak_pit_briefing(self, live: dict, tele: dict):
        """Speak the pre-pit briefing on the pit lap — deterministic, no AI query."""
        cs  = live.get('current_stint', {})
        ns  = live.get('next_stint',    {})
        opp = tele.get('opponents',     {})
        my_pos = opp.get('my_position')

        imperial = self._cfg.get('fuel_unit', 'gal') == 'gal'

        def _fmt_fuel(l):
            if l is None: return None
            return f"{l / 3.78541:.1f} gallons" if imperial else f"{l:.1f} litres"

        parts = ["Box this lap."]

        # Services for this stop — infer from plan
        if not ns or ns.get('is_last'):
            parts.append("Final stop.")
        else:
            next_driver  = ns.get('driver_name',  '')
            curr_driver  = cs.get('driver_name',  '')
            fuel_load    = ns.get('fuel_load')
            fuel_str     = _fmt_fuel(fuel_load)

            if next_driver and next_driver != curr_driver:
                parts.append(f"Driver change — {next_driver} gets in.")
            if fuel_str:
                parts.append(f"Taking {fuel_str}.")

        if my_pos:
            parts.append(f"Currently P{my_pos}.")

        msg = ' '.join(parts)
        self._say('pit_briefing', msg, 30)
        self.log(f'[PIT BRIEF] {msg}')

    def _ask_fuel_save_coaching(self, current_fpl: float, target_fpl: float,
                                 laps_to_save: int, fuel_remaining: float):
        """Ask the AI for specific fuel-save driving advice. Fire-and-forget."""
        if self._coaching_in_flight:
            return
        token = self._cfg.get('token', '')
        if not token:
            return
        with self._ctx_lock:
            ctx = self._ctx
        if not ctx:
            return

        reduction_pct = round((current_fpl - target_fpl) / current_fpl * 100)
        imperial      = self._cfg.get('fuel_unit', 'gal') == 'gal'

        def _fmt(l):
            return f"{l/3.78541:.2f}gal" if imperial else f"{l:.2f}L"

        system_prompt = self._build_system_prompt(ctx)
        question = (
            f"FUEL SAVE REQUIRED: currently burning {_fmt(current_fpl)}/lap, "
            f"need {_fmt(target_fpl)}/lap ({reduction_pct}% less) to make {laps_to_save} more laps "
            f"on {_fmt(fuel_remaining)} remaining. "
            f"Give 2 specific driving technique changes to hit the target — reference actual corners "
            f"on this track if known. Be direct and actionable, no more than 3 sentences."
        )

        self.log(f'[FUEL SAVE] Coaching requested: {_fmt(current_fpl)}/lap → {_fmt(target_fpl)}/lap')
        self._coaching_in_flight = True

        def _do():
            try:
                r = requests.post(
                    f'{BACKEND_URL}/engineer/coaching',
                    json={'token': token, 'system_prompt': system_prompt,
                          'question': question},
                    timeout=12,
                )
                if r.ok:
                    answer = r.json().get('answer', '')
                    if answer:
                        self.log(f'[FUEL SAVE] {answer}')
                        self.after(0, lambda: self._append_qa('[Fuel save] How do I save fuel?', answer))
                        self.speak(answer)
            except Exception as e:
                self.log(f'Fuel save coaching error: {e}')
            finally:
                self._coaching_in_flight = False

        threading.Thread(target=_do, daemon=True).start()

    def _on_pit_stop_complete(self, duration: float, fuel_added: float, sv_flags: int):
        """Called on main thread when player exits pit road."""
        services = []
        if sv_flags & 0x0001: services.append('LF tyre')
        if sv_flags & 0x0002: services.append('RF tyre')
        if sv_flags & 0x0004: services.append('LR tyre')
        if sv_flags & 0x0008: services.append('RR tyre')
        if sv_flags & 0x0010: services.append('fuel')
        if sv_flags & 0x0040: services.append('fast repair')
        planned = self._plan.get('pit_loss_s', 35)
        delta   = duration - planned
        svc_str = ' + '.join(services) if services else 'unknown'
        entry   = {'duration_s': round(duration, 1),
                   'fuel_added_l': round(fuel_added, 2),
                   'services': services}
        self._pit_stop_log.append(entry)
        if len(self._pit_stop_log) > 10:
            self._pit_stop_log.pop(0)
        msg = (f"Pit stop complete: {duration:.1f}s ({delta:+.1f}s vs plan). "
               f"Services: {svc_str}.")
        self.speak(msg)
        self.log(f'[PIT] {msg}')

    def _on_meatball_flag(self):
        """Fired when iRacing raises the meatball (serviceable/mechanical) flag."""
        if not self._running:
            return
        msg = ("Meatball flag — mechanical issue flagged. Check your mirrors and pit as soon as "
               "safely possible for inspection and repairs.")
        self.speak(msg)
        self.log(f'[FLAG] {msg}')
        threading.Thread(target=self._ask_damage_report, daemon=True).start()

    def _on_engine_warning(self, new_bits: int):
        """Fired when a new EngineWarnings bit appears — alerts driver to specific issue."""
        if not self._running:
            return
        labels = {
            0x01: 'water temperature warning',
            0x02: 'fuel pressure warning',
            0x04: 'oil pressure warning',
            0x08: 'engine stalled',
        }
        triggered = [labels[b] for b in (0x01, 0x02, 0x04, 0x08) if new_bits & b]
        if not triggered:
            return
        issue = ' and '.join(triggered)
        msg = f"Engine warning: {issue}. Monitor gauges and consider pitting."
        if self._say(f'engine_warn_{new_bits}', msg, 30):
            self.log(f'[ENGINE] {msg}')

    def _on_damage_detected(self, repair_s: float, opt_repair_s: float, fast_repair_avail):
        """Called on main thread when PitRepairLeft increases — new damage taken."""
        if not self._running:
            return
        fr_str = ''
        if fast_repair_avail is not None:
            avail = int(fast_repair_avail)
            if avail < 255:  # 255 = unlimited in iRacing
                fr_str = f' Fast repairs available: {avail}.'
        opt_str = f' Optional repair: {opt_repair_s:.0f}s.' if opt_repair_s > 0 else ''
        msg = (f"Damage alert: {repair_s:.0f}s mandatory repair time.{opt_str}{fr_str} "
               f"Consider if pitting now or waiting to bundle with your planned stop.")
        self.speak(msg)
        self.log(f'[DAMAGE] {msg}')
        threading.Thread(target=self._ask_damage_report, daemon=True).start()

    def _on_sector_delta(self, sector: str, delta: float):
        """Fires on main thread when a sector split is captured vs session best."""
        if not self._running:
            return
        if delta < -0.15:
            msg = f"Purple {sector}"
        elif delta < 0.0:
            msg = f"Green {sector}, {delta:+.2f}"
        elif delta < 0.4:
            msg = f"{sector}: {delta:+.2f}"
        else:
            msg = f"Lost time in {sector}, {delta:+.2f}"
        if self._say(f'sector_{sector.lower()}', msg, 5.0):
            self.log(f'[SECTOR] {sector}: {delta:+.3f}s vs session best')

    def _ask_damage_report(self):
        """Ask the AI for a damage assessment immediately after damage is detected."""
        try:
            with self._ctx_lock:
                ctx = self._ctx
            if not ctx:
                return
            token = self._cfg.get('token', '')
            if not token:
                return
            system_prompt = self._build_system_prompt(ctx)
            dmg = ctx.get('damage', {})
            repair_s = dmg.get('pit_repair_s', 0)
            opt_s    = dmg.get('pit_opt_repair_s', 0)
            question = (
                f"I just took damage. iRacing is showing {repair_s:.0f}s mandatory repair "
                f"and {opt_s:.0f}s optional repair. "
                f"Assess the damage situation and advise: should I pit immediately for repairs, "
                f"wait to bundle with my planned pit stop, or use a fast repair? "
                f"Consider current race position, fuel, and how much time the repairs would cost."
            )
            r = requests.post(
                f'{BACKEND_URL}/engineer/coaching',
                json={'token': token, 'system_prompt': system_prompt, 'question': question},
                timeout=15,
            )
            if r.ok:
                answer = r.json().get('answer', '')
                if answer:
                    self.speak(answer)
                    self.log(f'[DAMAGE AI] {answer}')
                    self._push_qa_to_pitwall(question, answer)
        except Exception as e:
            self.log(f'[DAMAGE] AI report failed: {e}')

    def _load_history_from_server(self):
        """Fetch session history from backend; fall back to local JSON on failure."""
        token = self._cfg.get('token', '')
        if not token:
            self._load_session_memory()
            return
        def _do():
            try:
                r = requests.get(
                    f'{BACKEND_URL}/engineer/history',
                    params={'token': token},
                    timeout=8,
                )
                if r.ok:
                    sessions = r.json().get('sessions', [])
                    if sessions:
                        lines = ['PAST SESSIONS (most recent first):']
                        for s in sessions[:5]:
                            best  = f"{s['best_lap_s']:.3f}s" if s.get('best_lap_s') else '?'
                            fpl   = f"{s['avg_fpl_l']:.2f}L/lap" if s.get('avg_fpl_l') else '?'
                            laps  = s.get('total_laps', '?')
                            lines.append(
                                f"- {s.get('session_date','?')} | {s.get('track_name','?')} | "
                                f"Best: {best} | FPL: {fpl} | {laps} laps"
                            )
                        self._session_memory_summary = '\n'.join(lines)
                        self.log(f'[HISTORY] Loaded {len(sessions)} past sessions.')
                        return
            except Exception:
                pass
            # Fallback to local JSON
            self._load_session_memory()
        threading.Thread(target=_do, daemon=True).start()

    def _add_session_note(self, note: str):
        self._session_notes.append(note)
        if len(self._session_notes) > 20:
            self._session_notes = self._session_notes[-20:]

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
            # Manual plans bypass _apply_auto_plan, so start the backend session here
            self._start_server_session(plan.get('track', plan.get('name', '')),
                                       plan.get('car', ''))
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

        # Reset all per-session state so a stop/restart is clean.
        self._last_coached_lap           = 0
        self._last_handling_coached_lap  = 0
        self._total_laps_this_session    = 0
        self._last_strategy_lap          = 0
        self._last_pit_briefed_stint     = 0
        self._last_fuel_save_lap         = 0
        self._last_pit_window_alert_laps = 999
        self._coaching_suppressed_until  = 0.0
        self._last_checkin_time          = 0.0
        self._session_best_lap           = 0.0
        self._lap_times_this_session     = []
        self._session_started            = False
        self._server_session_id          = None
        self._convo_history              = []
        self._coaching_in_flight         = False
        self._prev_session_type          = ''
        self._session_debrief_triggered  = False
        self._lap_sector_deltas          = {}
        self._per_lap_dynamics           = {}
        self._last_weather_alert         = 0.0
        self._last_overcut_alert         = 0.0
        self._last_weather_declared_wet  = False
        self._last_track_wetness         = 0
        self._last_track_temp_alerted_c  = None
        self._gap_history                = {'ahead': [], 'behind': []}
        self._last_blue_flag_alert       = 0.0
        self._prev_blue_flag             = False
        self._prev_meatball              = False
        self._pit_stop_log               = []
        self._prev_position          = None
        self._prev_incidents         = 0
        self._last_incident_alert    = 0.0
        self._last_gap_alert         = 0.0
        self._last_fuel_diverge_alert = 0.0
        self._last_position_alert    = 0.0
        self._last_driver_swap_alert = 0.0

        self._stop_evt.clear()
        self._running = True

        # ── Show/hide voice vs text input ─────────────────────────────────
        binding     = self._cfg.get('ptt_binding', DEFAULTS['ptt_binding'])
        btn_label   = _binding_label(binding)
        listen_mode = self._cfg.get('listen_mode', 'ptt')
        if voice_ok:
            self.text_input_frame.pack_forget()
            if listen_mode in ('vad', 'wake'):
                _wake = self._cfg.get('wake_word', DEFAULTS['wake_word'])
                _label = (f'● SAY  "{_wake.upper()}"…'
                          if listen_mode == 'wake' else '● LISTENING…')
                self.talk_label.config(fg=DIM, bg=BG, text=_label)
                self._start_vad_listener()
            else:
                self.talk_label.config(fg=DIM, bg=BG, text=f'HOLD  {btn_label}  TO  TALK')
                if binding.get('type') == 'joystick':
                    self._start_joystick_listener(binding)
                else:
                    self._start_keyboard_listener()
        else:
            self.talk_label.config(fg=BORDER, bg=BG, text='VOICE UNAVAILABLE — USE TEXT INPUT BELOW')
            self.text_input_frame.pack(fill='x', padx=14, pady=2)

        # ── Start threads ─────────────────────────────────────────────────
        self._tele_frame      = {}
        self._tele_lap_buf    = []
        self._tele_best_lap_s = 0.0
        self._callout_mgr = CalloutManager(self.speak, lambda msg: self.log(f'[CALLOUT] {msg}'))
        self._telemetry_thread = TelemetryThread(self)
        self._telemetry_thread.start()

        self._alert_gen += 1
        threading.Thread(target=self._alert_loop, args=(self._alert_gen,), daemon=True).start()

        if self._cfg.get('spotter_enabled', True):
            try:
                from spotter import SpotterThread
                self._spotter_thread = SpotterThread(
                    callout_mgr=self._callout_mgr,
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
        self._end_server_session()
        if len(self._lap_times_this_session) >= 5:
            self._do_session_debrief()
        self._stop_evt.set()
        self._running = False

        if self._telemetry_thread:
            self._telemetry_thread.stop()
            self._telemetry_thread = None

        # Clear stale context so queries asked after stopping don't get old data
        with self._ctx_lock:
            self._ctx = None

        if self._spotter_thread:
            self._spotter_thread.stop()
            self._spotter_thread = None

        if self._callout_mgr:
            self._callout_mgr.reset()
            self._callout_mgr = None

        self._stop_keyboard_listener()
        self._stop_vad_listener()
        self._stop_broadcast()
        self._joystick_thread = None  # daemon thread — exits when _stop_evt is set

        self.start_btn.config(state='normal')
        self.stop_btn.config(state='disabled')
        self.set_status('stopped')
        self.after(0, self._reset_talk_label)
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

        # Prefer sensor-based fuel % (fuel_level / capacity); fall back to plan estimate
        tele      = ctx.get('telemetry', {})
        sensor_l  = tele.get('fuel_level')
        capacity  = ctx.get('plan', {}).get('fuel_capacity_l')
        if sensor_l is not None and capacity:
            fuel_pct = round(sensor_l / capacity * 100)
        else:
            fuel_pct = live.get('fuel_pct')
        self._stint_vars['fuel'].set(f"{fuel_pct}%" if fuel_pct is not None else '—')

        laps_pit = live.get('laps_until_pit')
        mins_pit = live.get('mins_until_pit')
        if laps_pit is None:
            pit_str = '—'
            pit_time_str = '—'
        elif laps_pit < 0:
            pit_str = 'OVRD'
            pit_time_str = 'OVRD'
        else:
            pit_str = str(laps_pit)
            if mins_pit is not None:
                m = int(mins_pit)
                s = int((mins_pit - m) * 60)
                pit_time_str = f'{m}:{s:02d}'
            else:
                pit_time_str = '—'
        self._stint_vars['pit'].set(pit_str)
        self._stint_vars['pit_time'].set(pit_time_str)

        opp = ctx.get('telemetry', {}).get('opponents', {})
        my_pos = opp.get('my_position')
        self._stint_vars['pos'].set(f'P{my_pos}' if my_pos else '—')

        tele_last = tele.get('last_lap_time_s')
        if tele_last and tele_last > 0:
            m = int(tele_last) // 60
            s = tele_last - m * 60
            self._stint_vars['last_lap'].set(f'{m}:{s:06.3f}')
        else:
            self._stint_vars['last_lap'].set('—')

        # Update gap history for trend calculation in system prompt
        for side in ('ahead', 'behind'):
            o = opp.get(side)
            hist = self._gap_history[side]
            if o:
                hist.append(o['gap'])
                if len(hist) > 6:
                    hist.pop(0)
            else:
                self._gap_history[side] = []

    # ── Background: proactive alerts ─────────────────────────────────────────

    def _alert_loop(self, gen: int):
        while not self._stop_evt.is_set():
            self._stop_evt.wait(5)
            if self._stop_evt.is_set() or gen != self._alert_gen:
                break
            with self._ctx_lock:
                ctx = self._ctx
            if not ctx:
                continue
            tele_chk = ctx.get('telemetry', {})
            if tele_chk.get('stale'):
                continue
            # Suppress all proactive alerts when driver is not actively on track
            # (covers: sitting in pit box, spectating, garage, replay)
            if not tele_chk.get('is_on_track', True):
                continue

            live      = ctx.get('live', {})
            tele      = tele_chk
            now       = time.time()
            warn_laps = self._cfg.get('fuel_warning_laps', DEFAULTS['fuel_warning_laps'])
            coaching_suppressed = now < self._coaching_suppressed_until

            # Prefer live sensor + measured FPL for accuracy; fall back to plan estimate
            fuel_sensor   = tele.get('fuel_level')
            avg_fpl       = tele.get('fuel_delta', {}).get('avg_actual_fpl')
            if fuel_sensor is not None and avg_fpl:
                laps_of_fuel = fuel_sensor / avg_fpl
            else:
                laps_of_fuel = live.get('laps_of_fuel')

            laps_until_pit  = live.get('laps_until_pit')
            pit_status      = live.get('pit_window_status', '')
            pit_optimal     = live.get('pit_window_optimal', '?')
            current_lap_now = tele.get('current_lap', 0) or 0
            sess_type_now   = ctx.get('session', {}).get('type', '')

            # Fuel warning — only fire when we have 3+ measured laps to avoid plan-estimate false alarms
            fuel_laps_measured = tele.get('fuel_laps_measured', 0)
            if (laps_of_fuel is not None
                    and laps_of_fuel <= warn_laps
                    and fuel_laps_measured >= 3):
                msg = (
                    f"Fuel warning. {laps_of_fuel:.1f} laps of fuel remaining. "
                    f"Pit window is lap {pit_optimal}."
                )
                if self._say('fuel_warning', msg, 60):
                    self.log(f'[ALERT] {msg}')
                    self._last_fuel_alert = now

            # Approaching pit window — speak alert then fire strategy call
            if laps_until_pit is not None and 0 < laps_until_pit <= 2:
                msg = f"Approaching pit window. {laps_until_pit} laps to pit."
                if self._say('pit_window', msg, 60):
                    self.log(f'[ALERT] {msg}')
                    self._last_pit_alert = now

            # Pre-pit briefing — fires once on the pit lap itself
            current_stint_num = live.get('current_stint', {}).get('stint_num', 0) or 0
            if (laps_until_pit is not None and laps_until_pit <= 0
                    and pit_status != 'red'
                    and current_stint_num > self._last_pit_briefed_stint
                    and not tele.get('on_pit_road')):
                self._last_pit_briefed_stint = current_stint_num
                self._speak_pit_briefing(live, tele)

            # Fuel save coaching — fires when driver won't make pit window on current burn
            if (not coaching_suppressed
                    and fuel_laps_measured >= 3
                    and laps_until_pit is not None and laps_until_pit > 0
                    and laps_of_fuel is not None
                    and avg_fpl and fuel_sensor
                    and laps_of_fuel < laps_until_pit
                    and current_lap_now > self._last_fuel_save_lap + 5
                    and not self._coaching_in_flight):
                target_fpl = fuel_sensor / laps_until_pit
                if target_fpl > 0 and (avg_fpl - target_fpl) / avg_fpl > 0.05:
                    self._last_fuel_save_lap = current_lap_now
                    threading.Thread(
                        target=self._ask_fuel_save_coaching,
                        args=(avg_fpl, target_fpl, laps_until_pit, fuel_sensor),
                        daemon=True,
                    ).start()

            # Pit window countdown alerts — 5, 3, 2 laps out (before AI strategy fires at ≤1)
            if (not coaching_suppressed
                    and laps_until_pit is not None and pit_status in ('green', 'yellow')
                    and sess_type_now.lower() in ('race', 'feature race', 'heat race', 'lone qualify')
                    and not tele.get('on_pit_road')):
                _alert = self._last_pit_window_alert_laps
                if laps_until_pit == 5 and _alert > 5:
                    if self._say('pit_window_5', "Pit window in five laps", 120):
                        self.log('[PIT] Pit window 5 laps')
                    self._last_pit_window_alert_laps = 5
                elif laps_until_pit == 3 and _alert > 3:
                    if self._say('pit_window_3', "Pit window in three laps", 120):
                        self.log('[PIT] Pit window 3 laps')
                    self._last_pit_window_alert_laps = 3
                elif laps_until_pit == 2 and _alert > 2:
                    if self._say('pit_window_2', "Two laps to pit window", 120):
                        self.log('[PIT] Pit window 2 laps')
                    self._last_pit_window_alert_laps = 2

            # Proactive pit strategy — fires once when window opens (laps_until_pit == 0 or 1)
            if (not coaching_suppressed
                    and pit_status in ('open', 'green')
                    and laps_until_pit is not None and laps_until_pit <= 1
                    and sess_type_now.lower() in ('race', 'feature race', 'heat race', 'lone qualify')
                    and current_lap_now > self._last_strategy_lap + 3
                    and not self._coaching_in_flight):
                self._last_strategy_lap = current_lap_now
                threading.Thread(
                    target=self._ask_pit_strategy, args=('pit_window_opened',), daemon=True
                ).start()

            if pit_status == 'red':
                msg = "Overdue for pit stop. You are past the planned pit lap."
                if self._say('pit_overdue', msg, 120):
                    self.log(f'[ALERT] {msg}')
                    self._last_overdue_alert = now

            # Weather / track evolution alerts (advisory — suppressed by ack)
            weather = ctx.get('weather', {})
            wet_now     = weather.get('wet', False)
            wetness_now = weather.get('track_wetness', 0)
            if not coaching_suppressed and wet_now != self._last_weather_declared_wet:
                msg = ("Session declared wet — wet tyres now permitted."
                       if wet_now else "Session changed to dry conditions.")
                if self._say('weather_declared_wet', msg, 30):
                    self.log(f'[WEATHER] {msg}')
                    self._last_weather_declared_wet = wet_now
                    self._last_weather_alert = now
            elif not coaching_suppressed and wetness_now >= 4 and self._last_track_wetness < 4:
                msg = "Track conditions are getting wet. Consider your pit strategy."
                if self._say('weather_wet', msg, 60):
                    self.log(f'[WEATHER] {msg}')
                    self._last_track_wetness = wetness_now
                    self._last_weather_alert = now
            elif wetness_now < 2 and self._last_track_wetness >= 4:
                self._last_track_wetness = wetness_now  # track has dried, reset silently

            # Track temp change alert — fires when temp shifts ≥5°C from last alerted value
            track_temp_now = weather.get('track_temp_c')
            if track_temp_now is not None:
                if self._last_track_temp_alerted_c is None:
                    self._last_track_temp_alerted_c = track_temp_now
                elif not coaching_suppressed \
                        and abs(track_temp_now - self._last_track_temp_alerted_c) >= 5.0 \
                        and now - self._last_weather_alert > 120:
                    delta_t = track_temp_now - self._last_track_temp_alerted_c
                    direction = 'risen' if delta_t > 0 else 'dropped'
                    units = self._cfg.get('units_system', 'metric')
                    if units == 'imperial':
                        t_disp  = f"{_c_to_f(track_temp_now):.0f}°F"
                        d_disp  = f"{abs(delta_t * 9/5):.0f}°F"
                    else:
                        t_disp  = f"{track_temp_now:.0f}°C"
                        d_disp  = f"{abs(delta_t):.0f}°C"
                    msg = (f"Track temperature has {direction} by {d_disp} to {t_disp}. "
                           f"Tyre grip window may have shifted.")
                    if self._say('track_temp_change', msg, 300):
                        self.log(f'[WEATHER] {msg}')
                        self._last_track_temp_alerted_c = track_temp_now
                        self._last_weather_alert = now

            # Blue flag alert
            flags    = ctx.get('session_flags', {})
            blue_now = flags.get('blue', False)
            if blue_now and not self._prev_blue_flag:
                msg = "Blue flag. Let the leader through."
                if self._say('flag_blue', msg, 20):
                    self.log(f'[FLAG] {msg}')
                    self._last_blue_flag_alert = now
            self._prev_blue_flag = blue_now

            gap_history = self._gap_history
            ahead_hist  = gap_history.get('ahead', [])
            behind_hist = gap_history.get('behind', [])
            if not coaching_suppressed and now - self._last_gap_alert > 45:
                opp = tele.get('opponents', {})
                if len(behind_hist) >= 5:
                    behind_delta = behind_hist[-1] - behind_hist[-5]
                    b = opp.get('behind')
                    if behind_delta < -1.0 and b and 0 < b['gap'] < 20:
                        msg = (f"Car behind closing — {b['name']} is {b['gap']:.1f}s back "
                               f"and closing fast.")
                        if self._say('gap_closing_behind', msg, 45):
                            self.log(f'[GAP] {msg}')
                            self._last_gap_alert = now
                if len(ahead_hist) >= 5:
                    ahead_delta = ahead_hist[-1] - ahead_hist[-5]
                    a = opp.get('ahead')
                    if ahead_delta < -1.0 and a and 0 < a['gap'] < 20:
                        msg = (f"Closing on P{a['position']} — {a['gap']:.1f}s gap, "
                               f"gaining fast.")
                        if self._say('gap_closing_ahead', msg, 45):
                            self.log(f'[GAP] {msg}')
                            self._last_gap_alert = now

            my_pos_now = tele.get('opponents', {}).get('my_position')
            if (my_pos_now is not None
                    and self._prev_position is not None
                    and my_pos_now != self._prev_position):
                word = "Up" if my_pos_now < self._prev_position else "Down"
                msg = f"{word} to P{my_pos_now}."
                if self._say(f'position_{my_pos_now}', msg, 12):
                    self.log(f'[POS] {msg}')
                    self._last_position_alert = now
            if my_pos_now is not None:
                self._prev_position = my_pos_now

            fd       = tele.get('fuel_delta', {})
            avg_fpl  = fd.get('avg_actual_fpl')
            plan_fpl = ctx.get('plan', {}).get('fuel_per_lap_l')
            fuel_laps_measured = tele.get('fuel_laps_measured', 0)
            if (avg_fpl and plan_fpl and plan_fpl > 0 and fuel_laps_measured >= 3
                    and now - self._last_fuel_diverge_alert > 300):
                diverge_pct = abs(avg_fpl - plan_fpl) / plan_fpl * 100
                if diverge_pct >= 15:
                    direction = 'higher' if avg_fpl > plan_fpl else 'lower'
                    msg = (f"Fuel running {diverge_pct:.0f}% {direction} than planned — "
                           f"{avg_fpl:.2f}L/lap actual vs {plan_fpl:.2f}L planned.")
                    if direction == 'higher':
                        save_fpl = round(avg_fpl * 0.92, 2)
                        msg += f" Save mode target: {save_fpl:.2f}L/lap."
                        fuel_sensor_l = tele.get('fuel_level')
                        if fuel_sensor_l and laps_until_pit and laps_until_pit > 0:
                            laps_with_save = fuel_sensor_l / save_fpl
                            if laps_with_save >= laps_until_pit:
                                msg += " Should make the window on save."
                            else:
                                msg += " May need to pit earlier."
                    if self._say('fuel_diverge', msg, 300):
                        self.log(f'[FUEL] {msg}')
                        self._last_fuel_diverge_alert = now

            current_stint = live.get('current_stint', {})
            next_stint    = live.get('next_stint', {})
            if (next_stint
                    and current_stint.get('driver_name') != next_stint.get('driver_name')
                    and laps_until_pit is not None
                    and 0 < laps_until_pit <= 5
                    and now - self._last_driver_swap_alert > 120):
                next_driver = next_stint.get('driver_name', 'next driver')
                msg = f"Driver change in {laps_until_pit} laps. Get {next_driver} ready."
                if self._say('driver_swap', msg, 120):
                    self.log(f'[SWAP] {msg}')
                    self._last_driver_swap_alert = now

            incidents_now = tele.get('incidents', 0)
            if incidents_now > self._prev_incidents:
                msg = f"Incident! You're now on {incidents_now} incident point{'s' if incidents_now != 1 else ''}."
                if self._say(f'incident_{incidents_now}', msg, 0):
                    self.log(f'[INCIDENT] {msg}')
                    self._last_incident_alert = now
            self._prev_incidents = max(self._prev_incidents, incidents_now)

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

    # ── VAD always-on listener ────────────────────────────────────────────────

    def _start_vad_listener(self):
        if not SD_AVAILABLE or not SCIPY_AVAILABLE:
            self.log('VAD unavailable — sounddevice or scipy missing')
            return
        self._vad_thread = threading.Thread(target=self._vad_loop, daemon=True)
        self._vad_thread.start()
        self.log('Always-on VAD listener started.')

    def _stop_vad_listener(self):
        # Daemon thread exits automatically when _stop_evt fires; nothing explicit needed.
        self._vad_thread = None

    # ── Pit wall broadcast ────────────────────────────────────────────────────

    def _start_broadcast(self):
        if not self._server_session_id:
            return
        self._broadcast_thread = threading.Thread(target=self._broadcast_loop, daemon=True)
        self._broadcast_thread.start()
        self.log(f'Pit wall live at {BACKEND_URL}/engineer/pitwall/{self._server_session_id}')

    def _stop_broadcast(self):
        self._broadcast_thread = None  # daemon — exits with _stop_evt

    def _broadcast_loop(self):
        """Push one telemetry frame to the backend every 50 ms (20 fps)."""
        token = self._cfg.get('token', '')
        while not self._stop_evt.is_set() and self._running:
            frame = self._tele_frame
            sid   = self._server_session_id
            if frame and sid and token:
                with self._ctx_lock:
                    ctx = self._ctx
                meta = None
                if ctx:
                    meta = {
                        'track':       ctx.get('plan', {}).get('track', ''),
                        'car':         ctx.get('plan', {}).get('car', ''),
                        'driver_name': self._display_name or '',
                    }
                try:
                    requests.post(
                        f'{BACKEND_URL}/engineer/telemetry/push',
                        json={'token': token, 'session_id': sid, 'frame': frame,
                              'meta': meta},
                        timeout=2,
                    )
                except Exception:
                    pass
                # Accumulate frame in the current-lap buffer for ref-lap detection
                if frame.get('p') is not None:
                    self._tele_lap_buf.append(dict(frame))
            time.sleep(0.05)

    def _push_ref_lap(self, lap_time_s: float):
        """Send the accumulated lap buffer as the reference lap if it's the session best."""
        token = self._cfg.get('token', '')
        sid   = self._server_session_id
        if not token or not sid or not self._tele_lap_buf:
            return
        if self._tele_best_lap_s > 0 and lap_time_s >= self._tele_best_lap_s:
            return  # not a new best — don't overwrite the reference
        self._tele_best_lap_s = lap_time_s
        frames = list(self._tele_lap_buf)
        def _do():
            try:
                requests.post(
                    f'{BACKEND_URL}/engineer/telemetry/ref_lap',
                    json={'token': token, 'session_id': sid,
                          'frames': frames, 'lap_time_s': lap_time_s},
                    timeout=5,
                )
            except Exception:
                pass
        threading.Thread(target=_do, daemon=True).start()

    def _push_qa_to_pitwall(self, question: str, answer: str):
        """Forward a Q&A pair to the pit wall log."""
        token = self._cfg.get('token', '')
        sid   = self._server_session_id
        if not token or not sid:
            return
        def _do():
            try:
                requests.post(
                    f'{BACKEND_URL}/engineer/telemetry/qa',
                    json={'token': token, 'session_id': sid,
                          'question': question, 'answer': answer},
                    timeout=3,
                )
            except Exception:
                pass
        threading.Thread(target=_do, daemon=True).start()

    def _vad_loop(self):
        """Continuously monitors the mic. When sustained speech is detected, records and
        processes exactly like a PTT release — no external wake-word library required."""
        SAMPLE_RATE   = 16000
        CHUNK_FRAMES  = 1600            # 100 ms per chunk
        MIN_SPEECH_S  = 0.6            # ignore blips shorter than this
        MAX_RECORD_S  = 12.0           # safety cap — sends after 12 s even if speech continues
        SILENCE_S     = 1.2            # stop recording after this much silence
        silence_chunks = int(SILENCE_S * SAMPLE_RATE / CHUNK_FRAMES)
        min_speech_chunks = int(MIN_SPEECH_S * SAMPLE_RATE / CHUNK_FRAMES)

        # ── Noise-gate calibration: 1.5 s of ambient audio ────────────────
        self.after(0, lambda: self.talk_label.config(
            bg=BG3, fg=YELLOW, text='● CALIBRATING MIC…'))
        calibration_chunks = []
        try:
            with sd.InputStream(samplerate=SAMPLE_RATE, channels=1, dtype='float32',
                                blocksize=CHUNK_FRAMES) as stream:
                for _ in range(15):   # 15 × 100 ms = 1.5 s
                    chunk, _ = stream.read(CHUNK_FRAMES)
                    calibration_chunks.append(float(np.sqrt(np.mean(chunk ** 2))))
        except Exception as e:
            self.log(f'VAD calibration error: {e}')
            self.after(0, lambda: self.talk_label.config(
                bg=BG, fg=BORDER, text='VAD ERROR — CHECK MIC'))
            return
        ambient_rms = (sum(calibration_chunks) / len(calibration_chunks)) if calibration_chunks else 0.0
        user_thresh = self._cfg.get('vad_sensitivity', DEFAULTS['vad_sensitivity'])
        threshold   = max(ambient_rms * 3.0, user_thresh)
        self.log(f'VAD calibrated: ambient RMS={ambient_rms:.4f}, threshold={threshold:.4f}')
        self.after(0, lambda: self.talk_label.config(
            bg=BG, fg=DIM, text='● LISTENING…'))

        # ── Main detection loop ────────────────────────────────────────────
        try:
            with sd.InputStream(samplerate=SAMPLE_RATE, channels=1, dtype='float32',
                                blocksize=CHUNK_FRAMES) as stream:
                while not self._stop_evt.is_set() and self._running:
                    chunk, _ = stream.read(CHUNK_FRAMES)
                    rms = float(np.sqrt(np.mean(chunk ** 2)))

                    if rms < threshold:
                        continue    # silence — keep waiting

                    # Speech onset detected — collect until silence or max duration
                    if self._recording or self._coaching_in_flight:
                        continue    # busy — drop this onset
                    self.after(0, lambda: self.talk_label.config(
                        bg=ACCENT, fg='white', text='● LISTENING…'))
                    audio_buf  = [chunk.copy()]
                    silent_cnt = 0
                    max_chunks = int(MAX_RECORD_S * SAMPLE_RATE / CHUNK_FRAMES)

                    for _ in range(max_chunks):
                        if self._stop_evt.is_set() or not self._running:
                            break
                        chunk, _ = stream.read(CHUNK_FRAMES)
                        audio_buf.append(chunk.copy())
                        rms = float(np.sqrt(np.mean(chunk ** 2)))
                        if rms < threshold:
                            silent_cnt += 1
                            if silent_cnt >= silence_chunks:
                                break
                        else:
                            silent_cnt = 0

                    if len(audio_buf) < min_speech_chunks:
                        # Too short — probably a door slam or engine burst
                        self.after(0, lambda: self.talk_label.config(
                            bg=BG, fg=DIM, text='● LISTENING…'))
                        continue

                    # Hand off to the normal voice processing pipeline
                    audio = np.concatenate(audio_buf, axis=0).flatten()
                    audio_int16 = (audio * 32767).astype(np.int16)
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.wav') as tmp:
                        wav_path = tmp.name
                    wavfile.write(wav_path, SAMPLE_RATE, audio_int16)

                    self.after(0, lambda: self.talk_label.config(
                        bg=BG3, fg=YELLOW, text='● PROCESSING…'))
                    threading.Thread(
                        target=self._process_voice, args=(wav_path,), daemon=True).start()

                    # Wait until processing is done before listening again
                    while not self._stop_evt.is_set() and self._recording:
                        time.sleep(0.05)

        except Exception as e:
            self.log(f'VAD loop error: {e}')
        finally:
            if self._running:
                self.after(0, lambda: self.talk_label.config(
                    bg=BG, fg=BORDER, text='VAD STOPPED — CHECK MIC'))

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
        mode = self._cfg.get('listen_mode', 'ptt')
        if mode == 'vad':
            self.talk_label.config(bg=BG, fg=DIM, text='● LISTENING…')
        elif mode == 'wake':
            _wake = self._cfg.get('wake_word', DEFAULTS['wake_word'])
            self.talk_label.config(bg=BG, fg=DIM, text=f'● SAY  "{_wake.upper()}"…')
        else:
            binding   = self._cfg.get('ptt_binding', DEFAULTS['ptt_binding'])
            btn_label = _binding_label(binding)
            self.talk_label.config(bg=BG, fg=DIM, text=f'HOLD  {btn_label}  TO  TALK')

    def _stop_recording(self):
        if not self._recording:
            return
        self._recording = False
        self.after(0, lambda: self.talk_label.config(
            bg=BG3, fg=YELLOW, text='● PROCESSING…'))
        try:
            self._stream.stop()
            self._stream.close()
        except Exception:
            pass

        chunks = self._audio_chunks
        if not chunks:
            self.after(0, self._reset_talk_label)
            return

        def _save_and_process():
            try:
                audio = np.concatenate(chunks, axis=0).flatten()
                # Drop recordings shorter than 0.5 s — avoids wasting a query on a stray tap
                if len(audio) < 8000:
                    self.after(0, self._reset_talk_label)
                    return
                # NamedTemporaryFile avoids the TOCTOU race of mktemp()
                with tempfile.NamedTemporaryFile(delete=False, suffix='.wav') as tmp:
                    wav_path = tmp.name
                audio_int16 = (audio * 32767).astype(np.int16)
                wavfile.write(wav_path, 16000, audio_int16)
                self._process_voice(wav_path)
            except Exception as e:
                self.log(f'Recording save error: {e}')
                self.after(0, self._reset_talk_label)

        threading.Thread(target=_save_and_process, daemon=True).start()

    # ── Voice processing — via backend ────────────────────────────────────────

    def _process_voice(self, wav_path: str):
        token = self._cfg.get('token', '')
        if not token:
            self.log('Not logged in — cannot process voice')
            self.after(0, self._reset_talk_label)
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
                self.after(0, self._reset_talk_label)
                return
            question = r.json().get('transcript', '').strip()
            if not question:
                self.after(0, self._reset_talk_label)
                return
            # Wake word gate — if mode is 'wake', transcription must contain the phrase
            if self._cfg.get('listen_mode', 'ptt') == 'wake':
                wake = self._cfg.get('wake_word', DEFAULTS['wake_word']).lower()
                q_lower = question.lower()
                if wake not in q_lower:
                    self.log(f'[WAKE] Wake word not detected, ignoring utterance')
                    self.after(0, self._reset_talk_label)
                    return
                # Strip the wake phrase from the front of the question
                idx = q_lower.find(wake)
                question = question[idx + len(wake):].lstrip(' ,.')
                if not question:
                    self.after(0, self._reset_talk_label)
                    return
            self.log(f'You: "{question}"')
            # Acknowledgment gate — driver said "copy", "roger", etc.
            _ql = question.lower().rstrip('. ')
            _ACK_PHRASES = ('copy', 'roger', 'acknowledged', 'got it',
                            'affirmative', 'understood', 'noted', 'copy that')
            if any(_ql == p or _ql.startswith(p + ' ') or _ql.startswith(p + ',')
                   for p in _ACK_PHRASES):
                self._acknowledge()
                return
            # Route pit-strategy and fuel-save questions to focused handlers
            _pit_keywords    = ('pit', 'box', 'stop', 'stay out', 'extend')
            _strategy_intent = ('should', 'when', 'now', 'this lap', 'strategy', 'call')
            _save_keywords   = ('save fuel', 'fuel save', 'saving fuel', 'how much fuel',
                                 'make it', 'make the window', 'stretch')
            if any(k in _ql for k in _save_keywords):
                with self._ctx_lock:
                    _ctx_s = self._ctx
                _tele_s = (_ctx_s or {}).get('telemetry', {})
                _live_s = (_ctx_s or {}).get('live', {})
                _fpl_s  = _tele_s.get('fuel_delta', {}).get('avg_actual_fpl')
                _fuel_s = _tele_s.get('fuel_level')
                _ltp_s  = _live_s.get('laps_until_pit')
                if _fpl_s and _fuel_s and _ltp_s and _ltp_s > 0:
                    _target = _fuel_s / _ltp_s
                    threading.Thread(
                        target=self._ask_fuel_save_coaching,
                        args=(_fpl_s, _target, _ltp_s, _fuel_s),
                        daemon=True,
                    ).start()
                else:
                    self._ask_engineer(question)
            elif (any(k in _ql for k in _pit_keywords)
                    and any(k in _ql for k in _strategy_intent)):
                threading.Thread(
                    target=self._ask_pit_strategy, args=('driver_voice',), daemon=True
                ).start()
            else:
                self._ask_engineer(question)
        except Exception as e:
            self.log(f'Voice error: {e}')
            self.after(0, self._reset_talk_label)
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
        self._convo_history.append({'role': 'user', 'content': question})
        messages = self._convo_history[-6:]
        self.after(0, lambda: self.set_status('thinking'))
        try:
            r = requests.post(
                f'{BACKEND_URL}/engineer/ask',
                json={
                    'token':         token,
                    'system_prompt': system_prompt,
                    'question':      question,
                    'messages':      messages,
                },
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
            self._convo_history.append({'role': 'assistant', 'content': answer})
            if len(self._convo_history) > 20:
                self._convo_history = self._convo_history[-20:]
            self.log(f'Engineer: {answer}')
            self.log(f'  ({remaining} queries remaining today)')
            self.after(0, lambda: self.v_queries.set(
                f'{self._queries_today} / {self._query_limit} today  ({remaining} remaining)'
            ))
            self.after(0, lambda: self._append_qa(question, answer))
            self.speak(answer)
            self._push_qa_to_pitwall(question, answer)
        except Exception as e:
            self.log(f'Engineer error: {e}')
        finally:
            # Restore status dot and talk label (clears "PROCESSING…" / "Thinking…")
            with self._ctx_lock:
                has_ctx = bool(self._ctx)
            self.after(0, lambda: self.set_status('connected' if has_ctx else 'stopped'))
            self.after(0, self._reset_talk_label)

    def _build_system_prompt(self, ctx: dict) -> str:
        plan = ctx.get('plan', {})
        live = ctx.get('live', {})
        tele = ctx.get('telemetry', {})

        cs = live.get('current_stint', {})
        ns = live.get('next_stint', {})

        personality_key = self._cfg.get('personality', DEFAULT_PERSONALITY)
        persona_line    = PERSONALITY_PROMPTS.get(personality_key, PERSONALITY_PROMPTS[DEFAULT_PERSONALITY])

        units   = self._cfg.get('units_system', 'metric')
        imperial = (units == 'imperial')

        def safe_float(v, fmt='.1f'):
            try:
                return format(float(v), fmt)
            except (TypeError, ValueError):
                return str(v) if v is not None else '?'

        def fmt_temp(c):
            if c is None: return '?'
            v = _c_to_f(c) if imperial else c
            return f"{v:.0f}{'°F' if imperial else '°C'}"

        session = ctx.get('session', {})
        weather = ctx.get('weather', {})

        lines = [
            persona_line,
            "",
        ]

        if self._session_memory_summary:
            lines.append(self._session_memory_summary)
            lines.append("")

        # Track / car / session type
        track = plan.get('track', '')
        car   = plan.get('car', '')
        if track:
            lines.append(f"TRACK: {track}" + (f" | CAR: {car}" if car else ''))
        sess_type = session.get('type', '')
        if sess_type:
            lines.append(f"SESSION TYPE: {sess_type}")

        # Fuel display — prefer live iRacing sensor; show plan estimate alongside
        fuel_sensor_l = tele.get('fuel_level')  # raw sensor value from iRacing
        fuel_plan_l   = live.get('fuel_remaining_l')  # plan-calculated estimate

        def _fmt_fuel(litres):
            if litres is None:
                return '?'
            return f"{litres / 3.78541:.2f}gal" if imperial else f"{litres:.1f}L"

        fd_now = tele.get('fuel_delta', {})
        avg_fpl_now = fd_now.get('avg_actual_fpl')
        if fuel_sensor_l is not None and avg_fpl_now:
            sensor_laps = fuel_sensor_l / avg_fpl_now
        else:
            sensor_laps = None

        if fuel_sensor_l is not None:
            fuel_disp = _fmt_fuel(fuel_sensor_l)
        elif fuel_plan_l is not None:
            fuel_disp = f"{_fmt_fuel(fuel_plan_l)} (plan est.)"
        else:
            fuel_disp = '?'

        race_hrs = plan.get('race_duration_hrs', '?')
        try:
            race_hrs_fmt = (f"{float(race_hrs) * 60:.0f} min"
                            if isinstance(race_hrs, (int, float)) and race_hrs < 1
                            else f"{race_hrs}h")
        except (TypeError, ValueError):
            race_hrs_fmt = f"{race_hrs}h"

        # Build fuel line — never mix measured and plan-estimated values in the same line
        _cap = plan.get('fuel_capacity_l')
        _pct_str = f" | {round(fuel_sensor_l / _cap * 100)}%" if (fuel_sensor_l and _cap) else ""
        if sensor_laps is not None:
            _fuel_line = f"FUEL: {fuel_disp} remaining | {sensor_laps:.1f} laps (measured){_pct_str}"
        else:
            _fuel_line = f"FUEL: {fuel_disp} remaining | laps remaining: unknown (fuel burn not yet measured)"

        # Compute sensor-based last-safe pit lap when measured fuel data is available.
        # This overrides the plan-based estimate so the AI doesn't recommend a planned
        # pit lap that's beyond the actual fuel range.
        current_lap_num = tele.get('current_lap') or 0
        if sensor_laps is not None and isinstance(current_lap_num, (int, float)):
            sensor_last_safe = int(current_lap_num) + max(int(math.floor(sensor_laps)) - 1, 0)
            planned_last = live.get('pit_window_last')
            last_safe_display = str(sensor_last_safe)
            if isinstance(planned_last, (int, float)) and sensor_last_safe < int(planned_last):
                last_safe_display += f" (SENSOR — plan said {int(planned_last)}, DO NOT use plan value)"
        else:
            last_safe_display = str(live.get('pit_window_last', '?'))

        if live.get('status') == 'finished':
            lines += [
                f"RACE: {plan.get('name', 'Unknown')} | Duration: {race_hrs_fmt}",
                f"LAP: {live.get('current_lap', '?')} | STATUS: RACE COMPLETE — all planned stints finished",
            ]
        else:
            lines += [
                f"RACE: {plan.get('name', 'Unknown')} | Duration: {race_hrs_fmt}",
                f"LAP: {live.get('current_lap', '?')} | DRIVER: {cs.get('driver_name', '?')} "
                f"| STINT: {cs.get('stint_num', '?')} of {plan.get('total_stints', '?')}",
                _fuel_line,
                f"PIT WINDOW: lap {live.get('pit_window_optimal', '?')} "
                f"(last safe: {last_safe_display}) | "
                f"{live.get('laps_until_pit', '?')} laps away | "
                f"Status: {str(live.get('pit_window_status', '?')).upper()}",
                f"PIT LOSS: {plan.get('pit_loss_s', 35)}s (configured — verify for this track/car)",
            ]

        # Session time/laps remaining
        tr = session.get('time_remaining_s')
        lr = session.get('laps_remaining')
        if tr is not None and tr >= 0:
            if tr < 3600:
                lines.append(f"TIME REMAINING: {tr/60:.0f} min")
            else:
                lines.append(f"TIME REMAINING: {tr/3600:.2f}h ({tr/60:.0f} min)")
        elif lr is not None and lr >= 0:
            lines.append(f"LAPS REMAINING: {lr}")

        if ns:
            lines.append(
                f"NEXT DRIVER: {ns.get('driver_name', '?')} | Fuel load: {ns.get('fuel_load', '?')}L"
            )

        # Fuel delta — flag when data is still estimated
        fd = tele.get('fuel_delta', {})
        fuel_laps = tele.get('fuel_laps_measured', 0)
        if fd.get('avg_actual_fpl'):
            planned_fpl = plan.get('fuel_per_lap_l', '?')
            lines.append(
                f"FUEL DELTA: actual {fd['avg_actual_fpl']:.3f}L/lap vs planned {planned_fpl}L/lap"
                + (f" (measured over {fuel_laps} laps)" if fuel_laps >= 3 else "")
            )
        if fuel_laps < 3:
            lines.append(
                f"FUEL DATA WARNING: only {fuel_laps} of 3 minimum laps measured. "
                f"IMPORTANT: tell the driver you are still learning their fuel burn rate and "
                f"cannot give confident fuel strategy yet. Do not invent or guess laps-remaining."
            )

        # Damage report
        dmg = ctx.get('damage', {})
        repair_s     = dmg.get('pit_repair_s', 0)
        opt_repair_s = dmg.get('pit_opt_repair_s', 0)
        fr_avail     = dmg.get('fast_repair_avail')
        fr_used      = dmg.get('fast_repair_used', 0)
        eng_warn     = dmg.get('engine_warnings', {})
        water_c      = dmg.get('water_temp_c')
        oil_c        = dmg.get('oil_temp_c')
        oil_kpa      = dmg.get('oil_press_kpa')
        tire_wear    = dmg.get('tire_wear', {})

        if repair_s > 0 or opt_repair_s > 0:
            fr_str = ''
            if fr_avail is not None and int(fr_avail) < 255:
                fr_str = f' | Fast repairs: {fr_avail} available, {fr_used} used'
            lines.append(
                f"DAMAGE: {repair_s:.0f}s mandatory repair + {opt_repair_s:.0f}s optional{fr_str}"
            )
        else:
            lines.append("DAMAGE: none (PitRepairLeft = 0)")

        active_warnings = [k for k, v in eng_warn.items() if v]
        if active_warnings:
            lines.append(f"ENGINE WARNINGS: {', '.join(active_warnings).upper()}")
        if water_c is not None or oil_c is not None:
            vitals = []
            if water_c is not None: vitals.append(f"Water {fmt_temp(water_c)}")
            if oil_c is not None:   vitals.append(f"Oil {fmt_temp(oil_c)}")
            if oil_kpa is not None: vitals.append(f"Oil press {oil_kpa:.0f}kPa")
            lines.append(f"ENGINE VITALS: {' | '.join(vitals)}")

        worn = {c: w for c, w in tire_wear.items() if w is not None and w < 0.5}
        if worn:
            wear_str = '  '.join(f"{c}:{w*100:.0f}%" for c, w in worn.items())
            lines.append(f"TYRE WEAR (below 50%): {wear_str}")
        elif any(w is not None for w in tire_wear.values()):
            wear_str = '  '.join(f"{c}:{w*100:.0f}%" for c, w in tire_wear.items() if w is not None)
            lines.append(f"TYRE WEAR: {wear_str}")

        # Incidents
        incidents = tele.get('incidents', 0)
        if incidents:
            lines.append(f"INCIDENTS: {incidents}x")

        # Weather / track conditions
        if weather.get('track_temp_c') is not None or weather.get('air_temp_c') is not None:
            cond = f"CONDITIONS: Track {fmt_temp(weather.get('track_temp_c'))} | Air {fmt_temp(weather.get('air_temp_c'))}"
            if weather.get('wet'):
                cond += " | DECLARED WET"
            lines.append(cond)

        opponents = tele.get('opponents', {})
        if opponents:
            ahead   = opponents.get('ahead')
            behind  = opponents.get('behind')

            def _gap_trend(side):
                hist = self._gap_history.get(side, [])
                if len(hist) >= 5:
                    delta = hist[-1] - hist[-5]
                    if delta < -0.4:
                        return f', closing {abs(delta):.1f}s/5s'
                    if delta > 0.4:
                        return f', gap growing {delta:.1f}s/5s'
                return ''

            opp_parts = []
            if ahead:
                opp_parts.append(
                    f"P{ahead['position']} {ahead['name']} +{ahead['gap']:.1f}s ahead"
                    + _gap_trend('ahead')
                )
            if behind:
                opp_parts.append(
                    f"P{behind['position']} {behind['name']} +{behind['gap']:.1f}s behind"
                    + _gap_trend('behind')
                )
            if opp_parts:
                lines.append(f"OPPONENTS: {' | '.join(opp_parts)}")

        if self._pit_stop_log:
            last_ps     = self._pit_stop_log[-1]
            planned_pl  = plan.get('pit_loss_s', 35)
            delta_pl    = last_ps['duration_s'] - planned_pl
            svc         = '+'.join(last_ps['services']) or 'unknown'
            lines.append(
                f"LAST PIT STOP: {last_ps['duration_s']}s "
                f"({delta_pl:+.1f}s vs plan {planned_pl}s) | "
                f"Fuel added: {last_ps['fuel_added_l']:.1f}L | Services: {svc}"
            )
            if len(self._pit_stop_log) > 1:
                avg_dur = sum(p['duration_s'] for p in self._pit_stop_log) / len(self._pit_stop_log)
                lines.append(f"  Avg over {len(self._pit_stop_log)} stops: {avg_dur:.1f}s")

        lines.append("")
        lines.append("STINT PLAN SUMMARY:")
        stints_list = plan.get('stints', [])[:plan.get('total_stints', 99)]
        if not stints_list:
            lines.append("  (No stint plan loaded — waiting for iRacing session data or manual plan.)")
        else:
            for s in stints_list:
                marker  = "-> " if s.get('stint_num') == cs.get('stint_num') else "   "
                pit_str = f"pit lap {s['pit_lap']}" if s.get('pit_lap') else "FINAL"
                lines.append(
                    f"{marker}Stint {s['stint_num']}: {s.get('driver_name', '?')} "
                    f"laps {s['start_lap']}-{s['end_lap']} ({pit_str}) {s['fuel_load']}L"
                )

        # Championship context
        champ = plan.get('championship_context', {})
        if champ.get('enabled'):
            c_pts    = champ.get('current_points', 0)
            l_pts    = champ.get('points_leader_points', 0)
            gap      = l_pts - c_pts
            pts_tbl  = champ.get('points_per_position', [25, 18, 15, 12, 10, 8, 6, 4, 2, 1])
            pts_str  = ', '.join(f'P{i+1}={p}' for i, p in enumerate(pts_tbl[:6]))
            lines += [
                "",
                "CHAMPIONSHIP CONTEXT:",
                f"  Series: {champ.get('championship_name', '?')} "
                f"| Race {champ.get('race_number', '?')} of "
                f"{champ.get('races_remaining', '?')} remaining",
                f"  Your points: {c_pts} | "
                f"Leader: {champ.get('points_leader_name', 'P1')} ({l_pts} pts) | "
                f"Gap: {gap:+d} pts",
                f"  Scoring: {pts_str}",
                "  Consider championship position when advising on risk vs. reward.",
            ]

        temp_unit = '°F' if imperial else '°C'
        lines.append(f"\nUNITS: Always express temperatures in {temp_unit}.")

        return "\n".join(lines)

    # ── Proactive lap coaching ────────────────────────────────────────────────

    @staticmethod
    def _fmt_lap_spoken(s: float) -> str:
        m = int(s) // 60
        sec = s - m * 60
        if m > 0:
            return f"{m} minute{'s' if m != 1 else ''} {sec:.1f} seconds"
        return f"{sec:.1f} seconds"

    _RACE_SESSION_TYPES   = {'race'}
    _QUALI_SESSION_TYPES  = {'lone qualify', 'open qualify', 'qualify'}

    def _on_lap_complete(self, lap_num: int, lap_time: float, fpl, session_type: str = '',
                         sector_deltas: dict | None = None, lap_dynamics: dict | None = None):
        if lap_num <= self._last_coached_lap:
            return
        if lap_num < 2:
            self._last_coached_lap = lap_num
            return
        if not self._running:
            return
        st = session_type.lower() if session_type else ''
        # Skip entirely for practice / open-practice / unknown session types
        if st and st not in self._RACE_SESSION_TYPES and st not in self._QUALI_SESSION_TYPES:
            self._last_coached_lap = lap_num
            return

        if self._session_best_lap <= 0 or lap_time < self._session_best_lap:
            self._session_best_lap = lap_time

        self._total_laps_this_session += 1
        self._lap_times_this_session.append(lap_time)
        if len(self._lap_times_this_session) > 10:
            self._lap_times_this_session = self._lap_times_this_session[-10:]

        self._last_coached_lap = lap_num

        # Push ref lap if this is a new session best; reset lap buffer for next lap
        threading.Thread(
            target=self._push_ref_lap, args=(lap_time,), daemon=True).start()
        self._tele_lap_buf = []

        # Store sector deltas and dynamics for this lap
        if sector_deltas:
            self._lap_sector_deltas[lap_num] = sector_deltas
            if len(self._lap_sector_deltas) > 10:
                del self._lap_sector_deltas[min(self._lap_sector_deltas)]
        if lap_dynamics:
            self._per_lap_dynamics[lap_num] = lap_dynamics
            if len(self._per_lap_dynamics) > 10:
                del self._per_lap_dynamics[min(self._per_lap_dynamics)]

        # Read current position from context for the lap record
        with self._ctx_lock:
            ctx = self._ctx
        pos = ctx.get('telemetry', {}).get('opponents', {}).get('my_position') if ctx else None
        self._record_server_lap(lap_num, lap_time, fpl, pos)

        is_pb = (lap_time == self._session_best_lap and len(self._lap_times_this_session) > 1)
        recent = self._lap_times_this_session[-5:]
        avg    = sum(recent) / len(recent) if recent else lap_time
        is_slow = len(recent) >= 3 and lap_time > avg * 1.03
        _ci = self._cfg.get('checkin_laps', 5)
        is_check_in = bool(_ci) and (lap_num % _ci == 0)

        # Time-based check-in — fires when checkin_mins has elapsed since last check-in
        _cm = self._cfg.get('checkin_mins', 0)
        if _cm and not is_check_in:
            elapsed_since_checkin = time.time() - self._last_checkin_time
            if elapsed_since_checkin >= _cm * 60:
                is_check_in = True

        # Respect driver acknowledgment — suppress advisory coaching during snooze
        if time.time() < self._coaching_suppressed_until:
            is_check_in = False
            is_slow     = False

        if not (is_pb or is_slow or is_check_in):
            return

        if is_pb:
            reason = 'New session best!'
        elif is_slow:
            reason = f'Lap {(lap_time - avg):.1f}s slower than recent average'
        else:
            reason = 'Regular check-in'
            self._last_checkin_time = time.time()

        # Speak the lap time immediately so the driver hears it without network delay
        spoken_time = self._fmt_lap_spoken(lap_time)
        if is_pb:
            self._say(f'lap_{lap_num}', f"Lap {lap_num}, {spoken_time}. New session best.", 0)
        elif is_slow:
            self._say(f'lap_{lap_num}', f"Lap {lap_num}, {spoken_time}.", 0)
        else:
            self._say(f'lap_{lap_num}', f"Lap {lap_num} check-in, {spoken_time}.", 0)

        # Skip if a coaching request is already in flight — prevents API pile-up
        if self._coaching_in_flight:
            return
        self._coaching_in_flight = True
        threading.Thread(
            target=self._ask_lap_coaching,
            args=(lap_num, lap_time, fpl, reason, st,
                  self._lap_sector_deltas.get(lap_num),
                  self._per_lap_dynamics.get(lap_num)),
            daemon=True,
        ).start()

    def _ask_lap_coaching(self, lap_num: int, lap_time: float, fpl, reason: str,
                          session_type: str = '', sector_deltas: dict | None = None,
                          lap_dynamics: dict | None = None):
        recent   = self._lap_times_this_session[-5:]
        avg      = sum(recent) / len(recent) if recent else lap_time
        imperial = (self._cfg.get('units_system', 'metric') == 'imperial')
        if fpl is not None:
            fpl_val = f"{fpl / 3.78541:.3f}gal" if imperial else f"{fpl:.3f}L"
            fpl_str = f'FPL: {fpl_val}. '
        else:
            fpl_str = ''

        # Sector delta string — current lap + 3-lap trend per sector
        sector_str = ''
        if sector_deltas:
            parts = []
            for sx in ('s1', 's2', 's3'):
                if sx not in sector_deltas:
                    continue
                label = sx.upper()
                cur   = sector_deltas[sx]
                # Collect the same sector across the last 3 completed laps (excluding this one)
                recent_sector = [
                    self._lap_sector_deltas[ln][sx]
                    for ln in sorted(self._lap_sector_deltas)
                    if ln != lap_num and sx in self._lap_sector_deltas[ln]
                ][-3:]
                if len(recent_sector) >= 2:
                    trend_avg = sum(recent_sector) / len(recent_sector)
                    if cur > trend_avg + 0.05:
                        trend = f', worse than recent avg ({trend_avg:+.3f}s)'
                    elif cur < trend_avg - 0.05:
                        trend = f', improving vs recent avg ({trend_avg:+.3f}s)'
                    else:
                        trend = f', consistent with recent ({trend_avg:+.3f}s avg)'
                    parts.append(f"{label}: {cur:+.3f}s vs best{trend}")
                else:
                    parts.append(f"{label}: {cur:+.3f}s vs best")
            if parts:
                sector_str = f" Sector deltas — {', '.join(parts)}."

        # Consistency score across last 5 laps (std dev)
        consistency_str = ''
        if len(recent) >= 3:
            mean_t  = sum(recent) / len(recent)
            std_dev = (sum((t - mean_t) ** 2 for t in recent) / len(recent)) ** 0.5
            if std_dev > 1.5:
                consistency_str = (
                    f" Lap-to-lap consistency is poor (±{std_dev:.2f}s std dev over last "
                    f"{len(recent)} laps) — flag this to the driver."
                )
            elif std_dev < 0.4 and len(recent) >= 4:
                consistency_str = f" Driver is very consistent (±{std_dev:.2f}s std dev)."

        # Tire degradation: 4+ consecutive laps each slower than the previous
        deg_str = ''
        if len(self._lap_times_this_session) >= 4:
            tail = self._lap_times_this_session[-4:]
            if all(tail[i] < tail[i + 1] for i in range(len(tail) - 1)):
                deg_str = (
                    f" Pace has been declining for {len(tail)} consecutive laps "
                    f"(+{tail[-1] - tail[0]:.2f}s over that window) — likely tire degradation, "
                    f"not driver error."
                )

        # Handling tendencies string — suppress brake bias recs for 8 laps after one is given
        HANDLING_COOLDOWN = 8
        handling_str = ''
        if lap_dynamics:
            h_parts = []
            handling_within_cooldown = (
                lap_num - self._last_handling_coached_lap < HANDLING_COOLDOWN
                and self._last_handling_coached_lap > 0
            )
            if not handling_within_cooldown:
                if lap_dynamics.get('oversteer', 0) > 3:
                    h_parts.append(
                        f"oversteer: {lap_dynamics['oversteer']}% of lap "
                        f"(rear stepping out under braking — suggest moving brake bias forward, "
                        f"e.g. 'add 1 click of front brake bias')"
                    )
                if lap_dynamics.get('understeer', 0) > 3:
                    h_parts.append(
                        f"understeer: {lap_dynamics['understeer']}% of lap "
                        f"(front pushing wide — suggest moving brake bias rearward, "
                        f"e.g. 'add 1 click of rear brake bias')"
                    )
            if h_parts:
                handling_str = (
                    f" Handling notes: {', '.join(h_parts)}."
                    f" When recommending brake bias, always give a specific direction and number of "
                    f"clicks (1-2 at a time), e.g. 'add 1 click of front brake bias'."
                )
                self._last_handling_coached_lap = lap_num

        # Track name — used to prompt track-specific corner advice when sector data is present
        with self._ctx_lock:
            _ctx_snap = self._ctx
        track_name = (_ctx_snap or {}).get('plan', {}).get('track', '') if _ctx_snap else ''
        track_hint = (
            f" Sectors are approximate thirds of the lap at {track_name} — "
            f"when coaching sector-specific issues, name the relevant corners."
        ) if (track_name and sector_deltas) else ''

        is_quali = session_type in self._QUALI_SESSION_TYPES
        if is_quali:
            question = (
                f"Qualifying lap {lap_num} complete. Time: {self._fmt_lap_spoken(lap_time)}. "
                f"{reason}. Session best: {self._fmt_lap_spoken(self._session_best_lap)}. "
                f"Avg last 5: {self._fmt_lap_spoken(avg)}.{sector_str}{consistency_str}{track_hint} "
                f"This is a qualifying session — focus only on lap time and driving technique, "
                f"no race strategy or fuel. The lap time was already read out — do not repeat it."
            )
        else:
            question = (
                f"Lap {lap_num} complete. Time: {self._fmt_lap_spoken(lap_time)}. "
                f"{fpl_str}{reason}.{sector_str}{consistency_str}{deg_str}{handling_str}{track_hint} "
                f"Session best: {self._fmt_lap_spoken(self._session_best_lap)}. "
                f"Avg last 5: {self._fmt_lap_spoken(avg)}. "
                f"The lap time was already announced to the driver — give brief coaching only, "
                f"do not repeat the lap time. Always express times as minutes and seconds (e.g. "
                f"'1 minute 14 seconds'), never as total seconds."
            )
        self._add_session_note(f"Lap {lap_num}: {lap_time:.3f}s ({reason})")

        def _do():
            token = self._cfg.get('token', '')
            if not token:
                return
            with self._ctx_lock:
                ctx = self._ctx
            system_prompt = self._build_system_prompt(ctx) if ctx else ''
            try:
                r = requests.post(
                    f'{BACKEND_URL}/engineer/coaching',
                    json={'token': token, 'system_prompt': system_prompt,
                          'question': question},
                    timeout=12,
                )
                if r.ok:
                    answer = r.json().get('answer', '')
                    if answer:
                        self.log(f'[COACHING] {answer}')
                        self.after(0, lambda: self._append_qa(f'[Auto] {question}', answer))
                        self.speak(answer)
                        self._push_qa_to_pitwall(question, answer)
            except Exception as e:
                self.log(f'Coaching error: {e}')
            finally:
                self._coaching_in_flight = False

        threading.Thread(target=_do, daemon=True).start()

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
        """Queue text for speaking — never blocks, drops overflow to avoid backlog."""
        if self._muted:
            return
        try:
            self._tts_queue.put_nowait(text)
        except queue.Full:
            pass

    def _say(self, key: str, msg: str, cooldown_s: float = 0.0) -> bool:
        """Route a callout through CalloutManager for dedup, falling back to direct speak."""
        if self._callout_mgr:
            return self._callout_mgr.submit(key, msg, cooldown_s)
        self.speak(msg)
        return True

    @staticmethod
    def _clean_for_tts(text: str) -> str:
        """Strip markdown symbols that TTS engines read aloud as punctuation names."""
        import re
        text = re.sub(r'\*+', '', text)       # *, **
        text = re.sub(r'_+', '', text)        # _, __
        text = re.sub(r'`+', '', text)        # `, ```
        text = re.sub(r'#+\s*', '', text)     # #, ##, ### headings
        text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)  # [link text](url) → link text
        text = re.sub(r'\s{2,}', ' ', text)   # collapse extra whitespace left behind
        return text.strip()

    def _tts_worker(self):
        """Single persistent TTS thread — processes one utterance at a time."""
        sapi5_engine = None
        while True:
            text = self._tts_queue.get()
            if text is None:
                break
            text = self._clean_for_tts(text)
            voice_id = self._cfg.get('tts_voice', DEFAULT_VOICE)
            volume   = max(0.0, min(1.0, self._cfg.get('tts_volume', 1.0)))
            rate     = max(0.5, min(2.0, self._cfg.get('tts_rate', 1.0)))
            try:
                if voice_id == 'sapi5' or not EDGE_TTS_AVAILABLE:
                    # SAPI5 fallback — one persistent engine instance
                    if sapi5_engine is None and TTS_AVAILABLE:
                        sapi5_engine = pyttsx3.init()
                    if sapi5_engine:
                        sapi5_engine.setProperty('rate', int(175 * rate))
                        sapi5_engine.setProperty('volume', volume)
                        sapi5_engine.say(text)
                        sapi5_engine.runAndWait()
                else:
                    # edge-tts neural voice — save to temp MP3, play via pygame
                    try:
                        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.mp3')
                        tmp.close()
                        rate_pct = round((rate - 1.0) * 100)
                        rate_str = f'{rate_pct:+d}%'
                        asyncio.run(self._edge_tts_save(text, voice_id, tmp.name, rate_str))
                        if PYGAME_AVAILABLE:
                            if not pygame.mixer.get_init():
                                pygame.mixer.init()
                            pygame.mixer.music.load(tmp.name)
                            pygame.mixer.music.set_volume(volume)
                            pygame.mixer.music.play()
                            while pygame.mixer.music.get_busy():
                                time.sleep(0.05)
                            pygame.mixer.music.unload()
                        try:
                            os.remove(tmp.name)
                        except Exception:
                            pass
                    except Exception as e:
                        # edge-tts network failure — fall back to sapi5 so the
                        # driver still hears the message mid-race
                        self.log(f'edge-tts error (falling back to SAPI5): {e}')
                        if sapi5_engine is None and TTS_AVAILABLE:
                            sapi5_engine = pyttsx3.init()
                        if sapi5_engine:
                            sapi5_engine.setProperty('rate', int(175 * rate))
                            sapi5_engine.setProperty('volume', volume)
                            sapi5_engine.say(text)
                            sapi5_engine.runAndWait()
            except Exception as e:
                self.log(f'TTS error: {e}')

    @staticmethod
    async def _edge_tts_save(text: str, voice: str, path: str, rate: str = '+0%'):
        communicate = edge_tts.Communicate(text, voice, rate=rate)
        await communicate.save(path)

    # ── Status helpers ────────────────────────────────────────────────────────

    def set_status(self, status: str):
        colors = {
            'connected':  (GREEN,  'Connected — iRacing live'),
            'error':      (ACCENT, 'Connection error — retrying'),
            'stopped':    (BORDER, 'Stopped'),
            'connecting': (YELLOW, 'Connecting to iRacing…'),
            'thinking':   (CYAN,   'Thinking…'),
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
        self._end_server_session()
        if self._session_best_lap > 0 or self._lap_times_this_session:
            self._save_session_memory()
        self.stop_engineer()
        # Quit pygame fully so it releases all DLL handles before PyInstaller
        # cleans up the _MEI extraction directory — prevents cleanup warning
        try:
            if PYGAME_AVAILABLE and pygame.get_init():
                pygame.mixer.quit()
                pygame.quit()
        except Exception:
            pass
        self.destroy()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    app = App()
    app.mainloop()
