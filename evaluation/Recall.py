"""Helpers for computing recall against a baseline set of match projections."""

import csv
import os
from typing import Iterable, Set, Tuple, Dict

Projection = Tuple[int, int, int]


def write_projection_csv(path: str, projections: Iterable[Projection]) -> None:
    """Persist projection rows to CSV with a standard header.

    Note: This preserves duplicates and ordering as provided by the caller.
    """
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["a1_start", "last_a_end", "b_end"])
        for proj in projections:
            a, b, c = map(int, proj)
            writer.writerow((a, b, c))


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


def read_projection_counts(path: str) -> Dict[Projection, int]:
    """Load projections from CSV as a multiset (counts per triple), preserving duplicates.

    Returns a dict mapping Projection -> occurrence count.
    """
    counts: Dict[Projection, int] = {}
    with open(path, "r", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        header_skipped = False
        for row in reader:
            if not header_skipped:
                header_skipped = True
                continue
            if not row:
                continue
            triple = tuple(int(value) for value in row[:3])
            counts[triple] = counts.get(triple, 0) + 1
    return counts


def recall(baseline_csv: str, shedding_csv: str) -> float:
    """Compute recall between a baseline and shedding projection CSV using multiset semantics.

    - Baseline and shedding files may contain duplicate projection rows.
    - Recall is computed as the sum over triples of min(baseline_count, shedding_count)
      divided by the total number of baseline rows.
    """
    baseline_counts = read_projection_counts(baseline_csv)
    shedding_counts = read_projection_counts(shedding_csv)

    total_baseline = sum(baseline_counts.values())
    if total_baseline == 0:
        return 1.0

    matched = 0
    for triple, b_count in baseline_counts.items():
        s_count = shedding_counts.get(triple, 0)
        if s_count:
            matched += min(b_count, s_count)

    return matched / total_baseline
