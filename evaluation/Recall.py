import csv
import os
from typing import Iterable, Set, Tuple, Dict

Projection = Tuple[int, int, int]


def write_projection_csv(path: str, projections: Iterable[Projection]) -> None:

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
    # Compute recall of shedding run against baseline run.
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
