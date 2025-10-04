"""Utilities for recording latency stats and summarising runs."""

import csv
import os
import statistics
from typing import Iterable, List


def write_latency_csv(path: str, delays_ms: Iterable[float]) -> None:
    """Persist a list of latency samples (ms) to a single-column CSV."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["delay_ms"])
        for value in delays_ms:
            writer.writerow([f"{float(value):.3f}"])


def summary(delays_ms: List[float]) -> dict:
    """Calculate basic descriptive stats for latency samples."""
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
