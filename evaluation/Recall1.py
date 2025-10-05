"""Helpers for computing recall against a baseline set of match projections."""

import csv
import os
from typing import Iterable, Set, Tuple

Projection = Tuple[int, int, int]


def write_projection_csv(path: str, projections: Iterable[Projection]) -> None:
    """Persist a projection set to CSV with a standard header."""
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["a1_start", "last_a_end", "b_end"])
        for triple in sorted({tuple(map(int, proj)) for proj in projections}):
            writer.writerow(triple)


def read_projection_set(path: str) -> Set[Projection]:
    """Load projections from CSV produced by `write_projection_csv`."""
    projections: Set[Projection] = set()
    with open(path, "r", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        header_skipped = False
        for row in reader:
            if not header_skipped:
                header_skipped = True
                continue
            if not row:
                continue
            projections.add(tuple(int(value) for value in row[:3]))
    return projections


def recall(baseline_csv: str, shedding_csv: str) -> float:
    """Compute recall between a baseline and shedding projection CSV."""
    baseline = read_projection_set(baseline_csv)
    shedding = read_projection_set(shedding_csv)
    if not baseline:
        return 1.0
    return len(baseline & shedding) / len(baseline)
