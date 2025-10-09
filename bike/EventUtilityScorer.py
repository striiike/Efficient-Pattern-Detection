from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional, Sequence, Tuple


@dataclass
class _SequenceState:

    first_start: datetime
    last_end: datetime
    last_end_station: Optional[int]
    last_start_station: Optional[int]
    length: int


class BikeEventUtilityScorer:
    # Assigns an importance score to events for load shedding.

    __slots__ = ("_active_window", "_by_bike", "_target_stations")

    def __init__(
        self,
        target_stations: Optional[Sequence[int]] = None,
        active_window: timedelta = timedelta(hours=1),
    ) -> None:
        self._active_window = active_window
        self._target_stations = {int(station) for station in target_stations or ()}
        self._by_bike: Dict[str, _SequenceState] = {}

    def update_targets(self, target_stations: Sequence[int]) -> None:
        self._target_stations = {int(station) for station in target_stations}

    def update_window(self, active_window: timedelta) -> None:
        # Refresh the active window that bounds sequence relevance.
        self._active_window = active_window

    def score_event(
        self,
        bike_id: Optional[str],
        start_station: Optional[int],
        end_station: Optional[int],
        start_time: Optional[datetime],
        end_time: Optional[datetime],
    ) -> Tuple[float, str]:

        if bike_id is None or start_time is None:
            return 0.5, "supporting"

        self._prune_expired(start_time)
        score = 0.05  # Base score 

        state = self._by_bike.get(bike_id)
        if state is not None:
            score += 0.2
            if (
                start_station is not None
                and state.last_end_station is not None
                and start_station == state.last_end_station
                and start_time - state.last_end <= self._active_window
            ):
                score += 0.35
            elif (
                state.first_start is not None
                and start_time - state.first_start <= self._active_window
            ):
                score += 0.15

        if start_station is not None and start_station in self._target_stations:
            score += 0.15
        if end_station is not None and end_station in self._target_stations:
            score += 0.3

        if (
            end_time is not None
            and start_time is not None
            and end_time - start_time <= timedelta(minutes=15)
        ):
            score += 0.05

        score = max(0.0, min(1.0, score))
        if score >= 0.75:
            label = "critical"
        elif score >= 0.45:
            label = "supporting"
        else:
            label = "non_critical"
        return score, label

    def note_event(
        self,
        bike_id: Optional[str],
        start_station: Optional[int],
        end_station: Optional[int],
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        accepted: bool,
    ) -> None:
        if bike_id is None or start_time is None:
            return
        reference_time = end_time or start_time
        if reference_time is not None:
            self._prune_expired(reference_time)

        if not accepted:
            return

        state = self._by_bike.get(bike_id)
        if (
            state
            and start_station is not None
            and state.last_end_station is not None
            and start_station == state.last_end_station
            and start_time - state.last_end <= self._active_window
        ):
            state.last_end = end_time or start_time
            state.last_end_station = end_station
            state.last_start_station = start_station
            state.length += 1
        else:
            self._by_bike[bike_id] = _SequenceState(
                first_start=start_time,
                last_end=end_time or start_time,
                last_end_station=end_station,
                last_start_station=start_station,
                length=1,
            )

    def _prune_expired(self, current_time: datetime) -> None:
        cutoff = current_time - self._active_window
        stale = [
            bike
            for bike, state in self._by_bike.items()
            if state.last_end is not None and state.last_end < cutoff
        ]
        for bike in stale:
            self._by_bike.pop(bike, None)

