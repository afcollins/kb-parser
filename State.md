### **Context Handover: Project `kb-parse**`

**Objective:** A recursive Python parser for `kube-burner-ocp` results, designed to visualize scheduling bottlenecks (Serial vs. Parallel).

#### **1. File & Data Policy**

* **Discovery:** Recursive `os.walk('.')` to find pairs of `.log` and `collected-metrics-<UUID>/`.
* **Sources:** `podLatencyMeasurement-rds.json` (Latencies/Timestamps) and `jobSummary.json` (Metadata).
* **Missing Data:** Use a global `DEFAULT_VAL = "-"` for any null or missing values in the CSV/Summary.
* **Output File:** `kube-burner-ocp-final-report.csv`.

#### **2. Column Specifications (Strict Order)**

1. `OCP Version`, 2. `k-b version`, 3. `workers`, 4. `workload`, 5. `scheduler`, 6. `iterations`, 7. `podReplicas`, 8. `start time`, 9. `UUID`, 10. `p99`, 11. `max`, 12. `avg`, 13. `stddev`, 14. `end time`, 15. `percent`, 16. `duration`, 17. `cycles`, 18. `job_took`, 19. `sched_p90`, 20+. `P05` through `P95` (5% increments).

#### **3. Terminal Visualizations (`plotille`)**

* **Chronological Scatterplot:**
* **X-axis:** Seconds since start (Normalize by subtracting first pod timestamp).
* **Constraint:** **Force X-axis to start at 0.**
* **Y-axis:** `schedulingLatency` (ms).


* **Snap-to-Grid Histogram:**
* **Logic:** Manual quantization: `bucket = math.floor(latency / step) * step`.
* **Step Logic:** 1s (range < 30s), 2s (range < 90s), 10s (range < 300s).


* **CDF Plot:**
* **X-axis:** Latency (ms).
* **Y-axis:** Cumulative probability (0.0 to 1.0).
* **Constraint:** **Force both X and Y axes to start at 0.**



#### **4. Core Logic Snippets for Implementation**

```python
DEFAULT_VAL = "-"

# Pretty-bucket steps
def get_pretty_step(total_range_ms):
    range_sec = total_range_ms / 1000
    if range_sec <= 30: return 1000
    if range_sec <= 90: return 2000
    if range_sec <= 300: return 10000
    return 30000

# Plotting Constraints
fig.set_x_limits(min_=0) # Used for Scatter and CDF
fig.set_y_limits(min_=0) # Used for CDF

```
