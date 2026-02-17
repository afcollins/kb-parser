# Context Handover: Project `kb-parse`

**Objective:** A recursive Python parser for `kube-burner-ocp` benchmark results, optimized for scaling analysis of "topo-aware" scheduling.

### 1. Project Manifest & Requirements

* **Discovery:** Recursive `os.walk` search for pairs of a `.log` file and a `collected-metrics-<UUID>` directory in the same parent.
* **Data Sources:** * `podLatencyMeasurement-rds.json`: Primary scheduling latency array.
* `jobSummary.json`: Metadata (OCP version, scheduler, workers, replicas).


* **Output File:** `kube-burner-ocp-final-report.csv`
* **Column Order:** `OCP Version`, `k-b version`, `workers`, `workload`, `scheduler`, `iterations`, `podReplicas`, `start time`, `UUID`, `p99`, `max`, `avg`, `stddev`, `end time`, `percent`, `duration`, `cycles`, `job_took`, `sched_p90`, followed by `P05` through `P95` (5% steps).

### 2. Core Logic Snippets

#### A. Human-Friendly "Snap-to-Grid" Binning

This logic ensures histogram buckets align with human time-keeping and system timeouts rather than mathematical averages.

```python
def get_pretty_step(total_range_ms):
    range_sec = total_range_ms / 1000
    if range_sec <= 30: return 1000   # 1s buckets
    if range_sec <= 90: return 2000   # 2s buckets
    if range_sec <= 300: return 10000 # 10s buckets
    return 30000                      # 30s buckets

# Quantization formula to snap data to the grid
# snapped_value = math.floor(latency / step) * step

```

#### B. Dynamic Column Generation

```python
COLUMNS_START = [
    "OCP Version", "k-b version", "workers", "workload", "scheduler",
    "iterations", "podReplicas", "start time", "UUID", "p99",
    "max", "avg", "stddev", "end time", "percent", "duration", "cycles", "job_took", "sched_p90"
]
QUANTILES = [f"P{i:02d}" for i in range(5, 100, 5)]
COLUMN_ORDER = COLUMNS_START + QUANTILES

```

### 3. Current Performance Profile

Testing of the `topo-aware` scheduler has revealed a **Uniform Distribution** signature:

* **Histogram:** Characterized by "Flat" buckets (equal pod counts across the time range).
* **CDF:** A linear diagonal line (constant growth), indicating an  serial bottleneck where pods are processed one-by-one without parallel efficiency.
* **Stability:** High Variance labels () are expected in this linear growth model.
