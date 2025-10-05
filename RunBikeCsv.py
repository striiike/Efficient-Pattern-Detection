import argparse
import os
import time
from collections import Counter
from pathlib import Path

from CEP import CEP

from bike.BikeData import BikeDataFormatter
from bike.BikeHotPathPattern import create_bike_hot_path_pattern
from bike.BikeStream import TimingBikeInputStream, TimingBikeOutputStream
from evaluation.Metrics import (
    format_counters,
    summary,
    write_counters_csv,
    write_latency_csv,
)
from evaluation.Overload import OverloadDetector
from evaluation.Recall import recall as compute_recall, write_projection_csv

CSV_PATH_DEFAULT = "data/201804-citibike-tripdata_2.csv"
MAX_EVENTS_DEFAULT = 1000
OUTPUT_DIR = Path("bike/test_output")
OUTPUT_NAME = "csv_hot_path"
COUNTER_KEYS = (
    'events_ingested',
    'events_dropped',
    'matches_completed',
    'partial_pruned',
    'partial_evicted',
)


def _env_bool(name: str, default: bool) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return val.lower() not in {"0", "false", "no"}


def _clamp_01(value: float) -> float:
    return max(0.0, min(1.0, value))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run bike CSV pipeline with optional overload shedding and metrics logging."
    )
    parser.add_argument("--csv-path", default=CSV_PATH_DEFAULT, help="Input CSV path (default: %(default)s)")
    parser.add_argument("--max-events", type=int, default=MAX_EVENTS_DEFAULT, help="Maximum events to consume (default: %(default)s)")
    parser.add_argument("--shed", dest="shed", action="store_true", help="Enable load shedding")
    parser.add_argument("--no-shed", dest="shed", action="store_false", help="Disable load shedding")
    parser.add_argument("--shed-mode", choices=["event", "hybrid"], default="event",
                        help="'event' = drop input events; 'hybrid' = also shrink Kleene cap under load")
    parser.add_argument("--kleene-max", type=int, default=3, help="Maximum Kleene length for runs (default: %(default)s)")
    parser.add_argument("--time-window-hours", type=float, default=1.0, help="Time window in hours for the pattern (default: %(default)s)")
    parser.add_argument("--target-stations", type=str, help="Comma-separated list of target station IDs for b.end")
    parser.add_argument("--target-latency-ms", type=float, help="Override overload target in milliseconds")
    parser.add_argument("--drop-prob", type=float, help="Base drop probability when shedding")
    parser.add_argument("--sleep-ms", type=float, help="Per-event sleep in milliseconds")
    parser.add_argument("--burst-every", type=int, help="Sleep extra every N events")
    parser.add_argument("--burst-sleep", type=float, help="Additional sleep (ms) when burst trigger hits")
    parser.add_argument("--latency-csv", help="Path to latency CSV output")
    parser.add_argument("--projections-csv", help="Path to projection CSV output")
    parser.add_argument("--baseline-projections", help="Path to baseline projection CSV for recall calculation")
    parser.set_defaults(shed=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Parse target stations if provided
    target_stations = {426, 3002, 462}
    if args.target_stations:
        try:
            target_stations = {int(s.strip()) for s in args.target_stations.split(',') if s.strip()}
        except ValueError:
            print('Warning: could not parse --target-stations; using defaults {426,3002,462}')

    baseline_p50_ms = float(os.environ.get("BIKE_BASELINE_P50_MS", "50"))
    default_target = baseline_p50_ms * 0.5
    env_target = float(os.environ.get("BIKE_OVERLOAD_TARGET_MS", str(default_target)))
    target_latency_ms = args.target_latency_ms if args.target_latency_ms is not None else env_target

    env_shed = _env_bool("BIKE_SHED_ENABLED", True)
    shed_enabled = env_shed if args.shed is None else args.shed
    shed_mode = args.shed_mode

    env_drop_prob = _clamp_01(float(os.environ.get("BIKE_BASE_DROP_PROB", "0.0")))
    drop_prob = _clamp_01(args.drop_prob) if args.drop_prob is not None else env_drop_prob

    env_sleep_ms = float(os.environ.get("BIKE_SLEEP_MS", "0.0"))
    sleep_ms = args.sleep_ms if args.sleep_ms is not None else env_sleep_ms

    env_burst_every = int(os.environ.get("BIKE_BURST_EVERY", "0"))
    burst_every = args.burst_every if args.burst_every is not None else env_burst_every

    env_burst_sleep = float(os.environ.get("BIKE_BURST_SLEEP_MS", "0.0"))
    burst_sleep_ms = args.burst_sleep if args.burst_sleep is not None else env_burst_sleep

    run_counters = Counter({key: 0 for key in COUNTER_KEYS})
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    inp = TimingBikeInputStream(
        file_path=args.csv_path,
        max_events=args.max_events,
        use_test_data=False,
        enable_timing=True,
        shed_when_overloaded=shed_enabled,
        base_drop_prob=drop_prob,
        event_sleep_ms=sleep_ms,
        burst_every=burst_every,
        burst_sleep_ms=burst_sleep_ms,
        shed_mode=shed_mode,
        counters=run_counters,
    )
    out = TimingBikeOutputStream(
        inp,
        base_path=str(OUTPUT_DIR),
        file_name=OUTPUT_NAME,
        enable_timing=True,
    )

    overload_detector = OverloadDetector(target_latency_ms=target_latency_ms)
    out.overload_detector = overload_detector
    inp.overload_detector = overload_detector

    pattern, pattern_cfg = create_bike_hot_path_pattern(
        target_stations=target_stations,
        time_window_hours=args.time_window_hours,
        max_kleene_size=args.kleene_max,
    )
    if hasattr(inp, 'pattern_config'):
        inp.pattern_config = pattern_cfg
        pattern_cfg.reset()

    engine = CEP([pattern])

    start_ts = time.perf_counter()
    engine.run(inp, out, BikeDataFormatter())
    duration_s = time.perf_counter() - start_ts

    if overload_detector:
        print(f"\nOverload target latency: {target_latency_ms:.2f} ms")
        detection_latency = overload_detector.detection_latency_ms()
        if detection_latency is not None:
            print(f"Detection latency: {detection_latency:.2f} ms")
        else:
            print("Detection latency: (no overload detected)")
        print(f"Overloaded at completion: {overload_detector.overloaded}")

    if getattr(inp, "shed_when_overloaded", False):
        total_seen = getattr(inp, "total_events_seen", 0)
        dropped = getattr(inp, "dropped_events", 0)
        if total_seen:
            drop_pct = (dropped / total_seen) * 100.0
            print(f"Events dropped: {dropped} / {total_seen} ({drop_pct:.2f}%)")
        else:
            print("Events dropped: 0 / 0 (0.00%)")

    processed_events = getattr(inp, "event_count", 0)
    matches = len(getattr(out, "matches", []))
    delays_ms = [m.get("match_processing_delay_ms", 0.0) for m in out.matches if m.get("match_processing_delay_ms", 0.0) > 0]
    projections = {m.get("projection") for m in out.matches if m.get("projection")}

    throughput = processed_events / duration_s if duration_s > 0 else 0.0
    recall_proxy = (matches / processed_events) if processed_events else 0.0

    latency_csv = args.latency_csv or str(OUTPUT_DIR / f"latency_samples_{'shed' if shed_enabled else 'baseline'}_{OUTPUT_NAME}.csv")
    write_latency_csv(latency_csv, delays_ms)

    projections_csv = args.projections_csv or str(OUTPUT_DIR / f"projections_{'shed' if shed_enabled else 'baseline'}_{OUTPUT_NAME}.csv")
    write_projection_csv(projections_csv, projections)

    counters_csv = str(OUTPUT_DIR / f"counters_{'shed' if shed_enabled else 'baseline'}_{OUTPUT_NAME}.csv")
    write_counters_csv(counters_csv, run_counters)

    recall_value = None
    if args.baseline_projections:
        baseline_path = Path(args.baseline_projections)
        if baseline_path.exists():
            try:
                recall_value = compute_recall(str(baseline_path), projections_csv)
                print(f"\nRecall vs baseline: {recall_value:.3f}")
            except Exception as exc:
                print(f"\nRecall calculation failed: {exc}")
        else:
            print(f"\nBaseline projections file not found: {baseline_path}")

    stats = summary(delays_ms)
    if stats:
        print("\nLatency summary (ms):")
        for key in ("count", "p50", "p95", "avg", "min", "max"):
            if key in stats:
                print(f"  {key:>5}: {stats[key]:.2f}")
    else:
        print("\nNo latency samples recorded.")

    print("\nRun metrics:")
    print(f"  Throughput: {throughput:.2f} events/s")
    print(f"  Matches:    {matches}")
    print(f"  Recall*:    {recall_proxy:.3f} (matches/processed events)")
    if recall_value is not None:
        print(f"  Recall vs baseline: {recall_value:.3f}")
    print(f"  Latency CSV: {latency_csv}")
    print(f"  Projections CSV: {projections_csv}")
    if getattr(inp, 'pattern_config', None) and getattr(inp.pattern_config, 'max_kleene_size', None):
        print(f"  Final Kleene cap: {inp.pattern_config.max_kleene_size}")

    print("\nCounters:")
    print("  " + format_counters({key: run_counters.get(key, 0) for key in COUNTER_KEYS}))

    print("\n✅ Done.")
    print("Open:")
    print(f" - {OUTPUT_DIR / f'matches_{OUTPUT_NAME}.txt'}")
    print(f" - {OUTPUT_DIR / f'latencies_{OUTPUT_NAME}.txt'}")
    print(f" - {counters_csv}")
    print(f" - {latency_csv}")
    print(f" - {projections_csv}")


if __name__ == "__main__":
    main()
