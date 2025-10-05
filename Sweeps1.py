"""Latency-bound recall sweeps for bike pattern shedding."""

import argparse
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from collections import Counter

from CEP import CEP

from bike.BikeData import BikeDataFormatter
from bike.BikeHotPathPattern import create_bike_hot_path_pattern
from bike.BikeStream import TimingBikeInputStream, TimingBikeOutputStream
from evaluation.Metrics import summary
from evaluation.Overload import OverloadDetector

Projection = Tuple[int, int, int]
COUNTER_KEYS = ('events_ingested', 'events_dropped', 'matches_completed', 'partial_pruned', 'partial_evicted')


def _parse_caps(text: str) -> List[float]:
    parts = [p.strip() for p in text.split(",") if p.strip()]
    caps = []
    for part in parts:
        value = float(part)
        if value <= 0:
            raise ValueError("Caps must be positive fractions")
        caps.append(value)
    return caps


def _run_pipeline(
    *,
    csv_path: str,
    max_events: Optional[int],
    base_path: Path,
    file_suffix: str,
    shed_enabled: bool,
    target_latency_ms: Optional[float],
    base_drop_prob: float,
    event_sleep_ms: float,
    burst_every: int,
    burst_sleep_ms: float,
    shed_mode: str = 'event',
) -> Dict[str, object]:
    base_path.mkdir(parents=True, exist_ok=True)
    run_counters = Counter({key: 0 for key in COUNTER_KEYS})

    input_stream = TimingBikeInputStream(
        file_path=csv_path,
        max_events=max_events,
        use_test_data=False,
        enable_timing=True,
        shed_when_overloaded=shed_enabled,
        base_drop_prob=base_drop_prob,
        event_sleep_ms=event_sleep_ms,
        burst_every=burst_every,
        burst_sleep_ms=burst_sleep_ms,
    )

    output_stream = TimingBikeOutputStream(
        input_stream,
        base_path=str(base_path),
        file_name=f"sweeps_{file_suffix}",
        enable_timing=True,
    )

    overload_detector: Optional[OverloadDetector] = None
    if shed_enabled and target_latency_ms is not None:
        overload_detector = OverloadDetector(target_latency_ms=target_latency_ms)
        output_stream.overload_detector = overload_detector
        input_stream.overload_detector = overload_detector

    pattern, pattern_cfg = create_bike_hot_path_pattern(
        target_stations={426, 3002, 462},
        time_window_hours=1,
        max_kleene_size=3,
    )
    if hasattr(input_stream, 'pattern_config'):
        input_stream.pattern_config = pattern_cfg
        pattern_cfg.reset()
    engine = CEP([pattern])

    start_time = time.perf_counter()
    engine.run(input_stream, output_stream, BikeDataFormatter())
    duration_s = time.perf_counter() - start_time

    matches = output_stream.matches
    delays = [m.get("match_processing_delay_ms", 0.0) for m in matches if m.get("match_processing_delay_ms", 0.0) > 0]
    projections: Set[Projection] = {
        proj for proj in (m.get("projection") for m in matches) if proj
    }

    stats = summary(delays)
    throughput = (input_stream.event_count / duration_s) if duration_s > 0 else 0.0

    return {
        "stats": stats,
        "delays": delays,
        "projections": projections,
        "matches": len(matches),
        "events": input_stream.event_count,
        "throughput": throughput,
        "counters": run_counters,
        "detector": overload_detector,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sweep latency caps and measure recall under load shedding.")
    parser.add_argument("--csv-path", default="data/201804-citibike-tripdata_2.csv",
                        help="Input CSV path for the bike dataset (default: %(default)s)")
    parser.add_argument("--max-events", type=int, default=1000,
                        help="Limit the number of events consumed (default: %(default)s)")
    parser.add_argument("--caps", default="0.1,0.3,0.5,0.7,0.9",
                        help="Comma-separated latency caps as fractions of baseline median (default: %(default)s)")
    parser.add_argument("--drop-prob", type=float, default=0.0,
                        help="Base drop probability when shedding (default: %(default)s)")
    parser.add_argument("--sleep-ms", type=float, default=0.0,
                        help="Optional per-event sleep to simulate processing cost")
    parser.add_argument("--burst-every", type=int, default=0,
                        help="Inject an extra sleep every N events (default: %(default)s)")
    parser.add_argument("--burst-sleep-ms", type=float, default=0.0,
                        help="Sleep duration for burst injections (default: %(default)s)")
    parser.add_argument("--output-dir", default="bike/test_output/sweeps",
                        help="Directory for intermediate outputs (default: %(default)s)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    caps = _parse_caps(args.caps)
    base_path = Path(args.output_dir)

    print("Running baseline (no shedding)...")
    baseline_result = _run_pipeline(
        csv_path=args.csv_path,
        max_events=args.max_events,
        base_path=base_path,
        file_suffix="baseline",
        shed_enabled=False,
        target_latency_ms=None,
        base_drop_prob=args.drop_prob,
        event_sleep_ms=args.sleep_ms,
        burst_every=args.burst_every,
        burst_sleep_ms=args.burst_sleep_ms,
    )

    baseline_stats: Dict[str, float] = baseline_result["stats"]  # type: ignore[assignment]
    if not baseline_stats:
        print("No latency samples were recorded in the baseline run. Aborting sweep.")
        return

    baseline_p50 = baseline_stats.get("p50", 0.0)
    baseline_projections: Set[Projection] = baseline_result["projections"]  # type: ignore[assignment]

    print("\nBaseline summary:")
    print(f"  Events:     {baseline_result['events']}")
    print(f"  Matches:    {baseline_result['matches']}")
    print(f"  Median ms:  {baseline_p50:.2f}")
    print(f"  P95 ms:     {baseline_stats.get('p95', float('nan')):.2f}")
    print(f"  Throughput: {baseline_result['throughput']:.2f} events/s")

    if baseline_p50 <= 0.0:
        print("Baseline median latency is zero; skipping sweep as caps would collapse to zero.")
        return

    baseline_count = len(baseline_projections)

    rows: List[Tuple[float, float, float, float, float]] = []

    for cap in caps:
        target_ms = baseline_p50 * cap
        print(f"\nRunning shed sweep for cap {cap:.2f} (target {target_ms:.2f} ms)...")
        result = _run_pipeline(
            csv_path=args.csv_path,
            max_events=args.max_events,
            base_path=base_path,
            file_suffix=f"cap_{int(cap*100)}",
            shed_enabled=True,
            target_latency_ms=target_ms,
            base_drop_prob=args.drop_prob,
            event_sleep_ms=args.sleep_ms,
            burst_every=args.burst_every,
            burst_sleep_ms=args.burst_sleep_ms,
        )

        stats: Dict[str, float] = result["stats"]  # type: ignore[assignment]
        p50 = stats.get("p50", 0.0)
        p95 = stats.get("p95", 0.0)
        projections: Set[Projection] = result["projections"]  # type: ignore[assignment]
        recall = 1.0 if baseline_count == 0 else len(baseline_projections & projections) / baseline_count
        rows.append((cap * 100.0, target_ms, p50, p95, recall))

    if not rows:
        print("No sweep runs were executed.")
        return

    print("\nLatency-bound Recall Sweep:")
    header = f"{'Cap %':>6} {'Target ms':>12} {'Achieved p50':>14} {'Achieved p95':>14} {'Recall':>8}"
    print(header)
    print("-" * len(header))
    for cap_pct, target_ms, p50, p95, recall in rows:
        print(f"{cap_pct:6.1f} {target_ms:12.2f} {p50:14.2f} {p95:14.2f} {recall:8.3f}")


if __name__ == "__main__":
    main()
