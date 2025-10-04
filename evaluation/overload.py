"""Helpers for detecting overload conditions based on match processing latency."""

import time
from collections import deque
from typing import Deque, Optional


class OverloadDetector:
    """Track latency bursts using an exponential moving average.

    Args:
        target_latency_ms: EMA threshold that triggers overload.
        window_events: Number of last latency samples to retain for inspection.
        ema_alpha: Smoothing factor for the EMA (0 < alpha <= 1).
        exit_hysteresis: Multiplier applied to the target when clearing overload.
    """

    def __init__(self, target_latency_ms: float, window_events: int = 200, ema_alpha: float = 0.2, exit_hysteresis: float = 0.8) -> None:
        if target_latency_ms <= 0:
            raise ValueError("target_latency_ms must be positive")
        if not 0 < ema_alpha <= 1:
            raise ValueError("ema_alpha must be in (0, 1]")
        if not 0 < exit_hysteresis < 1:
            raise ValueError("exit_hysteresis must be between 0 and 1")
        if window_events <= 0:
            raise ValueError("window_events must be a positive integer")

        self.target_latency_ms = target_latency_ms
        self.window_events = window_events
        self.ema_alpha = ema_alpha
        self.exit_hysteresis = exit_hysteresis

        self.ema_latency: Optional[float] = None
        self.overloaded: bool = False
        self._overload_start_ts: Optional[float] = None
        self._burst_start_ts: Optional[float] = None
        self._history: Deque[float] = deque(maxlen=window_events)
        self._last_latency_ms: Optional[float] = None

    def note_latency(self, detection_latency_ms: float) -> None:
        """Record a latency sample and update overload state."""
        if detection_latency_ms is None:
            return

        self._last_latency_ms = detection_latency_ms

        if self.ema_latency is None:
            self.ema_latency = detection_latency_ms
        else:
            self.ema_latency = (
                self.ema_alpha * detection_latency_ms
                + (1.0 - self.ema_alpha) * self.ema_latency
            )

        self._history.append(detection_latency_ms)

        if self.ema_latency > self.target_latency_ms:
            if self._burst_start_ts is None:
                self._burst_start_ts = time.perf_counter()
            if not self.overloaded:
                self.overloaded = True
                self._overload_start_ts = time.perf_counter()
        elif self.overloaded and self.ema_latency <= self.target_latency_ms * self.exit_hysteresis:
            self.overloaded = False
            self._burst_start_ts = None
            self._overload_start_ts = None

    def detection_latency_ms(self) -> Optional[float]:
        """Return detection latency for the current burst, if available."""
        if self._burst_start_ts is None or self._overload_start_ts is None:
            return None
        return (self._overload_start_ts - self._burst_start_ts) * 1000.0

    def last_latency_ms(self) -> Optional[float]:
        """Expose the latest raw latency sample."""
        return self._last_latency_ms

    def history(self) -> Deque[float]:
        """Return the internal deque of latency samples."""
        return self._history

    def reset(self) -> None:
        """Clear overload state while keeping configuration intact."""
        self.ema_latency = None
        self.overloaded = False
        self._overload_start_ts = None
        self._burst_start_ts = None
        self._history.clear()
        self._last_latency_ms = None
