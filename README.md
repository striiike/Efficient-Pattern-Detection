# Efficient Pattern Detection over Data Streams

**CS-E4780 Scalable Systems and Data Management — Course Project 1**  
Group 7 — Baiyan Che, Amirreza Jafariandehkordi

---

## Overview

This project implements efficient pattern detection over streaming data using a SASE-style complex event processing (CEP) engine with load shedding capabilities.

### Pattern Definition (SASE-style)

```
PATTERN SEQ (BikeTrip+ a[], BikeTrip b)
WHERE a[i+1].bike = a[i].bike AND b.end IN {426, 3002, 462}
AND a[last].bike = b.bike AND a[i+1].start = a[i].end
WITHIN 1h
RETURN (a[1].start, a[i].end, b.end)
```

**Robustness enforced in code:**
- `b.end_time - a[1].start_time ≤ 1h` (and the engine's 1h window)

> **Note:** OpenCEP does not expose `RETURN` directly, so we compute the projection triple `(a1_start, last_a_end, b_end)` in the output stream for recall evaluation.

---

## Requirements

- **Python:** ≥ 3.13 (project uses standard libraries only)
- **Dataset:** NYC Citi Bike April 2018 second part (old schema with Bike ID)
  - Download from: https://s3.amazonaws.com/tripdata/index.html
  - Place CSV under `data/`
- **Outputs:** Written to `bike/test_output/` (created on first run)

---

## Setup

### 1. Create Virtual Environment & Install Dependencies

```bash
# Create virtual environment
python -m venv .venv

# Activate (macOS/Linux)
source .venv/bin/activate

# Activate (Windows PowerShell)
.\.venv\Scripts\Activate.ps1

# Install dependencies
# pip install ...
```

### 2. Prepare Dataset

Place the CSV file (old format with Bike ID) at:
```
data/201804-citibike-tripdata_2.csv
```

---

## Quick Start

All commands below use 1000 events. The workflow consists of:

1. **Synthetic sanity checks** (no shedding): Validate SEQ + Kleene + window correctness
2. **CSV baseline** (no shedding): Produce baseline latency and projections (reference for recall)
3. **Event-level shedding**: Check latency vs recall trade-off at 10/30/50/70/90% of baseline p50
4. **Hybrid shedding**: Show state-aware shedding (event-drop + dynamic Kleene cap shrink)
5. **Burst simulation**: Validate overload detection under periodic stalls
6. **(Optional) Sweep runner**: One-shot sweep with outputs under `bike/test_output/sweeps/`

---

## Usage

### Step 1: Synthetic Sanity Checks (No Shedding)

#### Kleene (+) Correct Path Detection

```bash
python RunSyntheticBike.py --no-shed --kleene --kleene-max 3
```

#### Fixed-Length Control (e.g., length 3)

```bash
python RunSyntheticBike.py --no-shed --fixed-length 3
```

**Expected behavior:** Valid chains to target stations (426/3002/462) within 1h produce matches; invalid cases (wrong bike, broken chaining, window violations) do not.

---

### Step 2: CSV Baseline (No Shedding)

This produces:
- Latency CSV (per-match samples)
- Baseline projections CSV (used to compute recall)
- Counters CSV (ingested/forwarded/dropped/matches)

```bash
python RunBikeCSV.py \
  --no-shed \
  --csv-path data/201804-citibike-tripdata_2.csv \
  --max-events 1000 \
  --kleene-max 3 \
  --latency-csv bike/test_output/latency_samples_baseline_csv_hot_path.csv \
  --projections-csv bike/test_output/projections_baseline_csv_hot_path.csv
```

**Check stdout:**
- "Latency summary (ms)" → note the **p50** (median)
- A non-zero "Matches"
- Paths to artifacts printed at the end

---

### Step 3: Event-Level Shedding + Recall

Use the baseline projections from Step 2. Replace `P50` below with the median you saw (example: P50=400).

```bash
# Set your baseline p50
P50=400
```

#### 10% Cap

```bash
python RunBikeCSV.py --shed --shed-mode event \
  --csv-path data/201804-citibike-tripdata_2.csv \
  --max-events 1000 \
  --kleene-max 3 \
  --target-latency-ms $(python - <<EOF
p50=$P50; print(p50*0.10)
EOF
) \
  --drop-prob 0.05 \
  --baseline-projections bike/test_output/projections_baseline_csv_hot_path.csv
```

#### 30% Cap

```bash
python RunBikeCSV.py --shed --shed-mode event \
  --csv-path data/201804-citibike-tripdata_2.csv \
  --max-events 1000 \
  --kleene-max 3 \
  --target-latency-ms $(python - <<EOF
p50=$P50; print(p50*0.30)
EOF
) \
  --drop-prob 0.05 \
  --baseline-projections bike/test_output/projections_baseline_csv_hot_path.csv
```

#### 50% Cap

```bash
python RunBikeCSV.py --shed --shed-mode event \
  --csv-path data/201804-citibike-tripdata_2.csv \
  --max-events 1000 \
  --kleene-max 3 \
  --target-latency-ms $(python - <<EOF
p50=$P50; print(p50*0.50)
EOF
) \
  --drop-prob 0.05 \
  --baseline-projections bike/test_output/projections_baseline_csv_hot_path.csv
```

#### 70% Cap

```bash
python RunBikeCSV.py --shed --shed-mode event \
  --csv-path data/201804-citibike-tripdata_2.csv \
  --max-events 1000 \
  --kleene-max 3 \
  --target-latency-ms $(python - <<EOF
p50=$P50; print(p50*0.70)
EOF
) \
  --drop-prob 0.05 \
  --baseline-projections bike/test_output/projections_baseline_csv_hot_path.csv
```

#### 90% Cap

```bash
python RunBikeCSV.py --shed --shed-mode event \
  --csv-path data/201804-citibike-tripdata_2.csv \
  --max-events 1000 \
  --kleene-max 3 \
  --target-latency-ms $(python - <<EOF
p50=$P50; print(p50*0.90)
EOF
) \
  --drop-prob 0.05 \
  --baseline-projections bike/test_output/projections_baseline_csv_hot_path.csv
```

**Check stdout for each cap:**
- "Overload target latency …"
- "Events dropped: X / 1000 (Y%)"
- "Latency summary (ms): p50/p95"
- "Recall vs baseline: …" (grading metric)

**Artifacts per run** (in `bike/test_output/`):
- `latency_samples_shed_csv_hot_path.csv`
- `projections_shed_csv_hot_path.csv`
- `counters_shed_csv_hot_path.csv`

---

### Step 4: Hybrid Shedding

This showcases state-aware shedding: under overload, the engine shrinks max Kleene length to control partial-state explosion, then recovers when load drops.

```bash
python RunBikeCSV.py --shed --shed-mode hybrid \
  --csv-path data/201804-citibike-tripdata_2.csv \
  --max-events 1000 \
  --kleene-max 4 \
  --target-latency-ms $(python - <<EOF
p50=$P50; print(p50*0.50)
EOF
) \
  --drop-prob 0.02 \
  --baseline-projections bike/test_output/projections_baseline_csv_hot_path.csv
```

**Check stdout:**
- Logs like `[HybridShedding] Kleene max size -> …`
- "Final Kleene cap: …"
- "Recall vs baseline: …"

---

### Step 5: Bursty Load Simulation

Inject periodic stalls to simulate bursty conditions and observe detection latency + p95 improvements under shedding.

```bash
python RunBikeCSV.py --shed --shed-mode event \
  --csv-path data/201804-citibike-tripdata_2.csv \
  --max-events 1000 \
  --kleene-max 3 \
  --drop-prob 0.05 \
  --burst-every 400 \
  --burst-sleep 300 \
  --target-latency-ms $(python - <<EOF
p50=$P50; print(p50*0.50)
EOF
) \
  --baseline-projections bike/test_output/projections_baseline_csv_hot_path.csv
```

**Expected:** "Detection latency" lines around bursts; p95 should tighten with more aggressive shedding.

---

### Step 6: One-Shot Sweep (Optional)

Run a baseline and sweep 10/30/50/70/90% automatically with outputs in a dedicated directory.

```bash
python Sweeps.py \
  --csv-path data/201804-citibike-tripdata_2.csv \
  --max-events 1000 \
  --caps 0.1,0.3,0.5,0.7,0.9 \
  --drop-prob 0.05 \
  --output-dir bike/test_output/sweeps
```

**Expected:** A summarized printout per cap, with p50/p95 and recall vs baseline. Artifacts (latency/projections/counters) are written under `bike/test_output/sweeps/`.

---


## Tips & Notes

### Reproducibility
If you enable shedding, consider seeding Python's RNG in the runner for consistent recall.

### Environment Variable Overrides
You can pass parameters via environment variables:
- `BIKE_BASE_DROP_PROB`
- `BIKE_SLEEP_MS`
- `BIKE_BURST_EVERY`
- `BIKE_BURST_SLEEP_MS`

### Output Artifacts

- **`counters_*.csv`** — ingested/forwarded/dropped/matches/evictions
- **`latency_samples_*.csv`** — per-match timings (min/avg/p50/p95 in stdout)
- **`projections_*.csv`** — `(a1_start, last_a_end, b_end)` tuples for recall evaluation

---

## License

This project is part of the CS-E4780 course at Aalto University.
