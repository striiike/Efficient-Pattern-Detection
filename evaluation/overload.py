"""Helpers for detecting overload conditions based on match and event processing latency."""

import time
from collections import deque
from typing import Deque, Optional


class OverloadEMA:
    """Track exponential moving averages for match and event latencies."""

    def __init__(self, alpha: float = 0.2, target_ms: float = 50.0) -> None:
        if not 0.0 < alpha <= 1.0:
            raise ValueError("alpha must be in (0, 1]")
        if target_ms <= 0.0:
            raise ValueError("target_ms must be positive")
        self.alpha = alpha
        self.target_ms = target_ms
        self.match_ms: Optional[float] = None
        self.event_ms: Optional[float] = None

    def update_match(self, value_ms: float) -> None:
        if value_ms is None:
            return
        if self.match_ms is None:
            self.match_ms = value_ms
        else:
            self.match_ms = (1.0 - self.alpha) * self.match_ms + self.alpha * value_ms

    def update_event(self, value_ms: float) -> None:
        if value_ms is None:
            return
        if self.event_ms is None:
            self.event_ms = value_ms
        else:
            self.event_ms = (1.0 - self.alpha) * self.event_ms + self.alpha * value_ms

    def latest(self) -> Optional[float]:
        values = [v for v in (self.match_ms, self.event_ms) if v is not None]
        if not values:
            return None
        return max(values)

    def overshoot(self) -> float:
        latest = self.latest()
        if latest is None or self.target_ms <= 0.0:
            return 0.0
        return max(0.0, (latest - self.target_ms) / self.target_ms)

    def is_overloaded(self, hysteresis: float = 0.05) -> bool:
        return self.overshoot() > hysteresis

    def reset(self) -> None:
        self.match_ms = None
        self.event_ms = None

    def update_target(self, target_ms: float) -> None:
        if target_ms <= 0.0:
            raise ValueError("target_ms must be positive")
        self.target_ms = target_ms


class OverloadDetector:
    """Track latency bursts using an exponential moving average across matches and events."""

    def __init__(
        self,
        target_latency_ms: float,
        window_events: int = 200,
        ema_alpha: float = 0.2,
        exit_hysteresis: float = 0.8,
    ) -> None:
        if target_latency_ms <= 0:
            raise ValueError("target_latency_ms must be positive")
        if not 0.0 < ema_alpha <= 1.0:
            raise ValueError("ema_alpha must be in (0, 1]")
        if not 0.0 < exit_hysteresis < 1.0:
            raise ValueError("exit_hysteresis must be between 0 and 1")
        if window_events <= 0:
            raise ValueError("window_events must be a positive integer")

        self.target_latency_ms = target_latency_ms
        self.window_events = window_events
        self.ema_alpha = ema_alpha
        self.exit_hysteresis = exit_hysteresis

        self.ema = OverloadEMA(alpha=ema_alpha, target_ms=target_latency_ms)
        self.ema_latency: Optional[float] = None
        self.overloaded: bool = False
        self._overload_start_ts: Optional[float] = None
        self._burst_start_ts: Optional[float] = None
        self._history: Deque[float] = deque(maxlen=window_events)
        self._last_latency_ms: Optional[float] = None

    def note_latency(self, detection_latency_ms: float) -> None:
        """Backward compatible alias for match latency updates."""
        self.note_match_latency(detection_latency_ms)

    def note_match_latency(self, detection_latency_ms: float) -> None:
        if detection_latency_ms is None:
            return
        self._last_latency_ms = detection_latency_ms
        self._history.append(detection_latency_ms)
        self.ema.update_match(detection_latency_ms)
        self._evaluate_state()

    def note_event_latency(self, processing_latency_ms: float) -> None:
        if processing_latency_ms is None:
            return
        self.ema.update_event(processing_latency_ms)
        self._evaluate_state()

    def overshoot(self) -> float:
        """Expose the current overshoot ratio relative to the target latency."""
        return self.ema.overshoot()

    def _evaluate_state(self) -> None:
        self.ema.update_target(self.target_latency_ms)
        latest = self.ema.latest()
        self.ema_latency = latest
        if latest is None:
            return

        if self._burst_start_ts is None and latest > self.target_latency_ms:
            self._burst_start_ts = time.perf_counter()

        if not self.overloaded and latest > self.target_latency_ms:
            self.overloaded = True
            self._overload_start_ts = time.perf_counter()
        elif self.overloaded and latest <= self.target_latency_ms * self.exit_hysteresis:
            self.overloaded = False
            self._burst_start_ts = None
            self._overload_start_ts = None

    def detection_latency_ms(self) -> Optional[float]:
        if self._burst_start_ts is not None and self._overload_start_ts is not None:
            return (self._overload_start_ts - self._burst_start_ts) * 1000.0
        return None

    def last_latency_ms(self) -> Optional[float]:
        return self._last_latency_ms

    def history(self) -> Deque[float]:
        return self._history

    def reset(self) -> None:
        self.ema.reset()
        self.ema_latency = None
        self.overloaded = False
        self._overload_start_ts = None
        self._burst_start_ts = None
        self._history.clear()
        self._last_latency_ms = None

    def update_target(self, target_latency_ms: float) -> None:
        if target_latency_ms <= 0:
            raise ValueError("target_latency_ms must be positive")
        self.target_latency_ms = target_latency_ms
        self.ema.update_target(target_latency_ms)
