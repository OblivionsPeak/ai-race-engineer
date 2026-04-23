"""
spotter.py — iRacing proximity and race awareness spotter module.

A CrewChief-equivalent background thread that monitors iRacing telemetry and
fires spoken callouts via a speak_fn callback. Import and use from ai_engineer.py:

    from spotter import SpotterThread
    spotter = SpotterThread(speak_fn=self.speak, log_fn=self.log)
    spotter.start()
    # later:
    spotter.stop()
"""

import threading
import time

# ---------------------------------------------------------------------------
# irsdk — optional import; spotter degrades gracefully if unavailable
# ---------------------------------------------------------------------------
try:
    import irsdk
    IRSDK_AVAILABLE = True
except ImportError:
    IRSDK_AVAILABLE = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POLL_INTERVAL = 0.25  # seconds

# CarLeftRight telemetry values
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

# SessionFlags bitmask
irsdk_checkered     = 0x0001
irsdk_white         = 0x0002
irsdk_green         = 0x0004
irsdk_yellow        = 0x0008
irsdk_red           = 0x0010
irsdk_blue          = 0x0020
irsdk_caution       = 0x4000
irsdk_cautionWaving = 0x8000

# Time-remaining thresholds (seconds) → callout text
TIME_THRESHOLDS = [
    (600, "Ten minutes remaining"),
    (300, "Five minutes remaining"),
    (120, "Two minutes remaining"),
    (60,  "One minute remaining"),
]

# Minimum seconds between proximity callouts for the same state
PROXIMITY_DEBOUNCE = 2.5

# Minimum seconds between position-change callouts
POSITION_DEBOUNCE = 3.0

# Gap callout thresholds (seconds)
GAP_AHEAD_THRESHOLD  = 3.0
GAP_BEHIND_THRESHOLD = 1.5

# "Lapped by" detection: if a car that was behind is now > 0.7 laps ahead
LAPPED_THRESHOLD = 0.7

# Default estimated lap time used for gap-to-seconds conversion
DEFAULT_LAP_S = 90.0

# Rapid re-appear window for "Still there, car left/right" callout (seconds)
RAPID_REAPPEAR_WINDOW = 1.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_lap_time(seconds: float) -> str:
    """Format a lap time in seconds as M:SS.mmm."""
    if seconds is None or seconds <= 0:
        return "--:--.---"
    minutes = int(seconds // 60)
    secs = seconds - minutes * 60
    return f"{minutes}:{secs:06.3f}"


def _delta_pct(their_pct: float, our_pct: float) -> float:
    """
    Signed lap-distance delta: positive means they are ahead of us.
    Handles wrap-around at the start/finish line.
    """
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
    spoken callouts via speak_fn.

    Parameters
    ----------
    speak_fn : callable(str)
        Called to speak a callout.  Must be thread-safe.
    log_fn : callable(str)
        Called to log events.  Must be thread-safe.
    """

    def __init__(self, speak_fn, log_fn):
        super().__init__(daemon=True, name="SpotterThread")
        self._speak = speak_fn
        self._log   = log_fn
        self._stop  = threading.Event()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def stop(self):
        """Signal the thread to exit cleanly."""
        self._stop.set()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _safe_speak(self, text: str):
        try:
            self._speak(text)
        except Exception:
            pass

    def _safe_log(self, text: str):
        try:
            self._log(text)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self):
        if not IRSDK_AVAILABLE:
            self._safe_log("Spotter: irsdk not available — spotter disabled.")
            return

        ir = irsdk.IRSDK()
        try:
            ir.startup()
        except Exception as e:
            self._safe_log(f"Spotter: irsdk startup error: {e}")

        # ---- State tracking ------------------------------------------
        prev_proximity      = None          # str from CAR_LEFT_RIGHT
        prev_position       = None          # int
        prev_lap_completed  = None          # int (None = not yet observed)
        prev_flags          = 0             # int bitmask
        prev_incidents      = None          # int (None = not yet observed)
        personal_best       = None          # float seconds

        last_proximity_call = 0.0           # time.time() of last proximity speak
        last_position_call  = 0.0           # time.time() of last position speak
        last_proximity_state = None         # state that was last spoken
        last_proximity_clear_time = 0.0     # when we last went clear

        time_alerts_fired   = set()         # set of threshold values already fired
        lapped_by           = set()         # set of car indices that have lapped us

        estimated_lap_s     = DEFAULT_LAP_S # updated from recent lap times
        gap_callout_lap     = -1            # last lap on which we fired a gap callout

        was_connected = False

        # ---- Main poll loop ------------------------------------------
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

                # ---- Read telemetry (all reads guarded individually) ----
                def _read(key):
                    try:
                        return ir[key]
                    except Exception:
                        return None

                car_lr_raw      = _read('CarLeftRight')
                lap_dist_pct    = _read('CarIdxLapDistPct')    # list[float]
                player_idx      = _read('PlayerCarIdx')         # int
                player_pos      = _read('PlayerCarPosition')    # int
                session_flags   = _read('SessionFlags')         # int
                last_lap_time   = _read('LapLastLapTime')       # float
                best_lap_time   = _read('LapBestLapTime')       # float
                session_remain  = _read('SessionTimeRemain')    # float
                lap_num         = _read('Lap')                  # int
                lap_completed   = _read('LapCompleted')         # int
                incidents       = _read('PlayerCarMyIncidentCount')  # int
                car_positions   = _read('CarIdxPosition')       # list[int]

                # =========================================================
                # 1. Proximity callouts
                # =========================================================
                if car_lr_raw is not None:
                    prox_state = CAR_LEFT_RIGHT.get(car_lr_raw, 'off')

                    if prox_state != prev_proximity:
                        # State changed — decide whether to speak
                        elapsed_since_call = now - last_proximity_call
                        do_speak = (elapsed_since_call >= PROXIMITY_DEBOUNCE
                                    or last_proximity_state != prox_state)

                        callout = None

                        if prox_state == 'clear':
                            if prev_proximity in ('left', 'right', 'both',
                                                  'left2', 'right2'):
                                callout = "Clear"
                            # Record when we went clear (for rapid re-appear check)
                            last_proximity_clear_time = now

                        elif prox_state == 'left':
                            # Rapid re-appear: clear→left within 1 s
                            if (prev_proximity == 'clear'
                                    and (now - last_proximity_clear_time)
                                    < RAPID_REAPPEAR_WINDOW):
                                callout = "Still there, car left"
                            else:
                                callout = "Car left"

                        elif prox_state == 'right':
                            if (prev_proximity == 'clear'
                                    and (now - last_proximity_clear_time)
                                    < RAPID_REAPPEAR_WINDOW):
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
                            self._safe_speak(callout)
                            last_proximity_call  = now
                            last_proximity_state = prox_state

                        prev_proximity = prox_state

                # =========================================================
                # 2. Lap completion callouts
                # =========================================================
                if lap_completed is not None:
                    if prev_lap_completed is None:
                        # First observation — just record, don't speak
                        prev_lap_completed = lap_completed
                    elif lap_completed > prev_lap_completed:
                        # New lap completed
                        prev_lap_completed = lap_completed

                        if last_lap_time and last_lap_time > 0:
                            fmt = _format_lap_time(last_lap_time)

                            # Update estimated lap time for gap calculations
                            estimated_lap_s = last_lap_time

                            is_pb = False
                            if personal_best is None or last_lap_time < personal_best:
                                personal_best = last_lap_time
                                is_pb = True

                            if is_pb:
                                self._safe_speak(f"Personal best! {fmt}")
                            else:
                                self._safe_speak(f"Last lap {fmt}")

                # =========================================================
                # 3. Position change callouts (not on lap 0/1)
                # =========================================================
                if (player_pos is not None
                        and lap_num is not None
                        and lap_num > 1):
                    if prev_position is None:
                        prev_position = player_pos
                    elif player_pos != prev_position:
                        elapsed = now - last_position_call
                        if elapsed >= POSITION_DEBOUNCE:
                            if player_pos < prev_position:
                                self._safe_speak(f"Up to P{player_pos}")
                            else:
                                self._safe_speak(f"Back to P{player_pos}")
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
                        """True if this flag bit newly appeared."""
                        return bool(changed & mask) and bool(new_flags & mask)

                    if _flag_raised(irsdk_caution) or _flag_raised(irsdk_cautionWaving) \
                            or _flag_raised(irsdk_yellow):
                        self._safe_speak("Yellow flag, caution")
                    elif _flag_raised(irsdk_green):
                        self._safe_speak("Green flag, go go go")
                    elif _flag_raised(irsdk_checkered):
                        self._safe_speak("Checkered flag, that's the race")
                    elif _flag_raised(irsdk_white):
                        self._safe_speak("White flag, final lap")
                    elif _flag_raised(irsdk_blue):
                        self._safe_speak("Blue flag, move aside")

                    prev_flags = new_flags

                # =========================================================
                # 5. Gap callouts (once per lap, after lap 1)
                # =========================================================
                if (lap_dist_pct is not None
                        and player_idx is not None
                        and lap_completed is not None
                        and lap_completed > 1
                        and lap_completed != gap_callout_lap):

                    try:
                        our_pct = lap_dist_pct[player_idx]
                        best_ahead  = None   # smallest positive delta
                        best_behind = None   # smallest negative delta (abs)

                        for i, their_pct in enumerate(lap_dist_pct):
                            if i == player_idx:
                                continue
                            if their_pct <= 0.0:
                                # Car not on track / pits
                                continue
                            delta = _delta_pct(their_pct, our_pct)
                            if delta > 0:
                                if best_ahead is None or delta < best_ahead:
                                    best_ahead = delta
                            elif delta < 0:
                                if best_behind is None or abs(delta) < abs(best_behind):
                                    best_behind = delta

                        if best_ahead is not None:
                            gap_ahead_s = best_ahead * estimated_lap_s
                            if gap_ahead_s < GAP_AHEAD_THRESHOLD:
                                self._safe_speak(
                                    f"Gap ahead, {gap_ahead_s:.1f} seconds"
                                )

                        if best_behind is not None:
                            gap_behind_s = abs(best_behind) * estimated_lap_s
                            if gap_behind_s < GAP_BEHIND_THRESHOLD:
                                self._safe_speak(
                                    f"Watch behind, {gap_behind_s:.1f} seconds"
                                )

                        gap_callout_lap = lap_completed

                    except Exception:
                        pass

                # =========================================================
                # 6. Incident count callouts
                # =========================================================
                if incidents is not None:
                    if prev_incidents is None:
                        prev_incidents = incidents
                    elif incidents > prev_incidents:
                        prev_incidents = incidents
                        self._safe_speak(f"Incident. You're at {incidents}x")
                        if incidents >= 17:
                            self._safe_speak("Warning, approaching incident limit")

                # =========================================================
                # 7. Time-remaining callouts
                # =========================================================
                if session_remain is not None and session_remain > 0:
                    for threshold, message in TIME_THRESHOLDS:
                        if (threshold not in time_alerts_fired
                                and session_remain <= threshold):
                            self._safe_speak(message)
                            time_alerts_fired.add(threshold)

                # =========================================================
                # 8. Blue flag / being lapped detection
                # =========================================================
                if (lap_dist_pct is not None
                        and player_idx is not None
                        and lap_num is not None
                        and lap_num > 1):

                    try:
                        our_pct = lap_dist_pct[player_idx]
                        for i, their_pct in enumerate(lap_dist_pct):
                            if i == player_idx:
                                continue
                            if their_pct <= 0.0:
                                continue
                            delta = _delta_pct(their_pct, our_pct)
                            # Car is now ahead by more than 0.7 laps →
                            # we have been lapped by them
                            if delta > LAPPED_THRESHOLD and i not in lapped_by:
                                lapped_by.add(i)
                                self._safe_speak("Blue flag, let them through")
                    except Exception:
                        pass

            except Exception as e:
                # Never crash the spotter — log and keep going
                self._safe_log(f"Spotter error: {e}")

            self._stop.wait(POLL_INTERVAL)

        # Clean up
        try:
            ir.shutdown()
        except Exception:
            pass
        self._safe_log("Spotter: thread stopped.")
