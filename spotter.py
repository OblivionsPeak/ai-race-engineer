"""
spotter.py — iRacing proximity and race awareness spotter module.

Handles: proximity (car left/right), position changes, race flags, lap times,
time-remaining callouts, and lapped-car detection.

Does NOT handle: incidents (owned by main alert loop), gap-ahead/behind callouts
(main app uses CarIdxF2Time which is more accurate than lap-distance estimation).

All callouts route through a CalloutManager for dedup/cooldown so neither the
spotter nor the main alert loop can double-fire the same event.
"""

import threading
import time

try:
    import irsdk
    IRSDK_AVAILABLE = True
except ImportError:
    IRSDK_AVAILABLE = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POLL_INTERVAL = 0.25  # seconds

CAR_LEFT_RIGHT = {
    0: 'off',
    1: 'off',
    2: 'clear',
    3: 'left',
    4: 'right',
    5: 'both',
    6: 'left2',
    7: 'right2',
}

irsdk_checkered     = 0x0001
irsdk_white         = 0x0002
irsdk_green         = 0x0004
irsdk_yellow        = 0x0008
irsdk_red           = 0x0010
irsdk_blue          = 0x0020
irsdk_caution       = 0x4000
irsdk_cautionWaving = 0x8000

TIME_THRESHOLDS = [
    (600, "Ten minutes remaining"),
    (300, "Five minutes remaining"),
    (120, "Two minutes remaining"),
    (60,  "One minute remaining"),
]

PROXIMITY_DEBOUNCE   = 2.5
POSITION_DEBOUNCE    = 3.0
RAPID_REAPPEAR_WINDOW = 1.0
LAPPED_THRESHOLD     = 0.7


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_lap_time(seconds: float) -> str:
    if seconds is None or seconds <= 0:
        return "--:--.---"
    minutes = int(seconds // 60)
    secs = seconds - minutes * 60
    return f"{minutes}:{secs:06.3f}"


def _delta_pct(their_pct: float, our_pct: float) -> float:
    delta = their_pct - our_pct
    if delta > 0.5:
        delta -= 1.0
    elif delta < -0.5:
        delta += 1.0
    return delta


# ---------------------------------------------------------------------------
# SpotterThread
# ---------------------------------------------------------------------------

class SpotterThread(threading.Thread):
    """
    Background thread that polls iRacing telemetry every 250 ms and fires
    spoken callouts via a CalloutManager.

    Parameters
    ----------
    callout_mgr : CalloutManager
        Shared dedup/cooldown gate — must have a .submit(key, msg, cooldown_s) method.
    log_fn : callable(str)
        Called to log events. Must be thread-safe.
    """

    def __init__(self, callout_mgr, log_fn):
        super().__init__(daemon=True, name="SpotterThread")
        self._cm   = callout_mgr
        self._log  = log_fn
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def _safe_log(self, text: str):
        try:
            self._log(text)
        except Exception:
            pass

    def _say(self, key: str, msg: str, cooldown_s: float = 0.0):
        try:
            self._cm.submit(key, msg, cooldown_s)
        except Exception:
            pass

    def run(self):
        if not IRSDK_AVAILABLE:
            self._safe_log("Spotter: irsdk not available — spotter disabled.")
            return

        ir = irsdk.IRSDK()
        try:
            ir.startup()
        except Exception as e:
            self._safe_log(f"Spotter: irsdk startup error: {e}")

        prev_proximity            = None
        prev_position             = None
        prev_lap_completed        = None
        prev_flags                = 0

        last_proximity_call       = 0.0
        last_position_call        = 0.0
        last_proximity_state      = None
        last_proximity_clear_time = 0.0

        time_alerts_fired         = set()

        was_connected = False

        while not self._stop.is_set():
            try:
                connected = ir.is_initialized and ir.is_connected
                if not connected:
                    if was_connected:
                        self._safe_log("Spotter: iRacing disconnected, retrying…")
                        was_connected = False
                    try:
                        ir.startup()
                    except Exception:
                        pass
                    self._stop.wait(2.0)
                    continue

                if not was_connected:
                    self._safe_log("Spotter: connected to iRacing.")
                    was_connected = True

                ir.freeze_var_buffer_latest()
                now = time.time()

                def _read(key):
                    try:
                        return ir[key]
                    except Exception:
                        return None

                car_lr_raw      = _read('CarLeftRight')
                lap_dist_pct    = _read('CarIdxLapDistPct')
                player_idx      = _read('PlayerCarIdx')
                player_pos      = _read('PlayerCarPosition')
                session_flags   = _read('SessionFlags')
                session_remain  = _read('SessionTimeRemain')
                lap_num         = _read('Lap')
                lap_completed   = _read('LapCompleted')

                # =========================================================
                # 1. Proximity callouts
                # =========================================================
                if car_lr_raw is not None:
                    prox_state = CAR_LEFT_RIGHT.get(car_lr_raw, 'off')

                    if prox_state != prev_proximity:
                        elapsed_since_call = now - last_proximity_call
                        do_speak = (elapsed_since_call >= PROXIMITY_DEBOUNCE
                                    or last_proximity_state != prox_state)

                        callout = None

                        if prox_state == 'clear':
                            if prev_proximity in ('left', 'right', 'both', 'left2', 'right2'):
                                callout = "Clear"
                            last_proximity_clear_time = now

                        elif prox_state == 'left':
                            if (prev_proximity == 'clear'
                                    and (now - last_proximity_clear_time) < RAPID_REAPPEAR_WINDOW):
                                callout = "Still there, car left"
                            else:
                                callout = "Car left"

                        elif prox_state == 'right':
                            if (prev_proximity == 'clear'
                                    and (now - last_proximity_clear_time) < RAPID_REAPPEAR_WINDOW):
                                callout = "Still there, car right"
                            else:
                                callout = "Car right"

                        elif prox_state == 'both':
                            callout = "Car left, car right"

                        elif prox_state == 'left2':
                            callout = "Two cars left"

                        elif prox_state == 'right2':
                            callout = "Two cars right"

                        if callout and do_speak:
                            self._say(f'proximity_{prox_state}', callout, PROXIMITY_DEBOUNCE)
                            last_proximity_call  = now
                            last_proximity_state = prox_state

                        prev_proximity = prox_state

                # Lap completion tracking — no callout here; main app owns lap announcements
                if lap_completed is not None:
                    if prev_lap_completed is None:
                        prev_lap_completed = lap_completed
                    else:
                        prev_lap_completed = lap_completed

                # =========================================================
                # 3. Position change callouts (not on lap 0/1)
                # =========================================================
                if player_pos is not None and lap_num is not None and lap_num > 1:
                    if prev_position is None:
                        prev_position = player_pos
                    elif player_pos != prev_position:
                        elapsed = now - last_position_call
                        if elapsed >= POSITION_DEBOUNCE:
                            word = "Up" if player_pos < prev_position else "Back"
                            self._say(f'position_{player_pos}', f"{word} to P{player_pos}", POSITION_DEBOUNCE)
                            last_position_call = now
                        prev_position = player_pos
                elif player_pos is not None and prev_position is None:
                    prev_position = player_pos

                # =========================================================
                # 4. Race flag callouts
                # =========================================================
                if session_flags is not None:
                    new_flags = session_flags
                    changed   = new_flags ^ prev_flags

                    def _flag_raised(mask):
                        return bool(changed & mask) and bool(new_flags & mask)

                    if _flag_raised(irsdk_caution) or _flag_raised(irsdk_cautionWaving) \
                            or _flag_raised(irsdk_yellow):
                        self._say('flag_yellow', "Yellow flag, caution", 30)
                    elif _flag_raised(irsdk_green):
                        self._say('flag_green', "Green flag, go go go", 10)
                    elif _flag_raised(irsdk_checkered):
                        self._say('flag_checkered', "Checkered flag, that's the race", 0)
                    elif _flag_raised(irsdk_white):
                        self._say('flag_white', "White flag, final lap", 0)
                    elif _flag_raised(irsdk_blue):
                        self._say('flag_blue', "Blue flag, move aside", 10)

                    prev_flags = new_flags

                # =========================================================
                # 5. Time-remaining callouts
                # =========================================================
                if session_remain is not None and session_remain > 0:
                    for threshold, message in TIME_THRESHOLDS:
                        if threshold not in time_alerts_fired and session_remain <= threshold:
                            self._say(f'time_{threshold}', message, 0)
                            time_alerts_fired.add(threshold)

                # Blue flag / lapped detection: handled by main app using SessionFlags.blue
                # which is the authoritative iRacing field. Lap-distance estimation
                # produces false positives and is removed from the spotter.

            except Exception as e:
                self._safe_log(f"Spotter error: {e}")

            self._stop.wait(POLL_INTERVAL)

        try:
            ir.shutdown()
        except Exception:
            pass
        self._safe_log("Spotter: thread stopped.")
