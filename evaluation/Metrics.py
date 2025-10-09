import csv
import os
import statistics
from typing import Iterable, List, Mapping, Any


def write_latency_csv(path: str, delays_ms: Iterable[float]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["delay_ms"])
        for value in delays_ms:
            writer.writerow([f"{float(value):.3f}"])


def summary(delays_ms: List[float]) -> dict:
    # Calculate basic descriptive stats for latency samples.
    samples = list(delays_ms)
    if not samples:
        return {}
    samples.sort()
    result = {
        "count": len(samples),
        "p50": statistics.median(samples),
        "avg": sum(samples) / len(samples),
        "min": samples[0],
        "max": samples[-1],
    }
    if len(samples) >= 20:
        # statistics.quantiles requires at least n samples
        result["p95"] = statistics.quantiles(samples, n=20)[18]
    else:
        # fallback: approximate via percentile index
        index = max(0, int(round(0.95 * (len(samples) - 1))))
        result["p95"] = samples[index]
    return result


def write_counters_csv(path: str, counters: Mapping[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["name", "value"])
        for key in sorted(counters):
            writer.writerow([key, counters[key]])


def format_counters(counters: Mapping[str, Any]) -> str:
    # Return a human-readable string for counter values.
    return ", ".join(f"{key}: {counters[key]}" for key in sorted(counters))
