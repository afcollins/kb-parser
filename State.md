**Project:** `kb-parse` — A Python-based recursive parser for Kube-Burner-OCP performance metrics.

**Core Requirements:**

1. **File Discovery:** Use `os.walk` to recursively find collocated pairs of a `kube-burner-ocp-<UUID>.log` file and a directory named `collected-metrics-<UUID>`.
2. **Target Files:**
* Logs: Extract start/end times, version, UUID, and job duration.
* Metrics: `podLatencyMeasurement-rds.json` (Primary latencies) and `jobSummary.json` (Metadata).


3. **Calculations:**
* Standard metrics (Avg, Max, p99, StdDev).
* Consistency: Coefficient of Variation (CV = StdDev / Avg).
* Stability Labels: ✅ Stable (CV<0.2), ⚠️ Noisy (CV<0.5), ❌ High Variance (CV>0.5).
* Quantiles: Programmatic generation of P05 through P95 in 5% increments.


4. **Terminal Visualization:** * `plotille`-based Frequency Histogram with clean, 1000ms-aligned integer buckets.
* CDF (Cumulative Distribution) Braille-style line plot.


5. **Output Format:**
* `OUTPUT_FILE = "kube-burner-ocp-final-report.csv"`
* **Column Order:** `OCP Version`, `k-b version`, `workers`, `workload`, `scheduler`, `iterations`, `podReplicas`, `start time`, `UUID`, `p99`, `max`, `avg`, `stddev`, `end time`, `percent`, `duration`, `cycles`, `job_took`, `sched_p90`, followed by `P05-P95`.
* Final output must print the Summary Table (including `Replicas` and `CV`) followed by the raw CSV data to `stdout`.

**Current Progress:** Successfully identifying a linear O(n) bottleneck in `topo-aware` scheduling characterized by a Uniform Distribution (flat histogram/diagonal CDF).
