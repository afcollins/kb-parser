# kb-parser

CLI tools for parsing and visualizing performance metrics from **kube-burner-ocp** test runs.

## Install

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
make install
# or: uv tool install .
```

This installs three commands: `kb-parser`, `container-stats`, `pod-latency-stats`.

## Usage

### kb-parser

Analyze latency and generic metric files with stats, histograms, and CDF plots.

```bash
# Direct file mode — pass any metric JSON file
kb-parser containerCPU.json
kb-parser podLatencyMeasurement-workload.json
kb-parser containerCPU.json --group-by container --top-labels 5

# UUID fragment mode — discovers files from kube-burner output dirs
kb-parser 2178a534
kb-parser 2178a534 metrics containerCPU

# Common flags
#   --no-visuals    Stats only, no plots
#   --scatter       Add scatter plot (time vs value)
#   -S              Show label cardinality
#   -b MIN,MAX      Value range filter
#   -t START,END    Time range filter (seconds)
#   -l KEY=VALUE    Label filter
#   --group-by KEY  Per-group stats and plots
#   --top-labels N  Limit groups shown (default 10)
#   -L TYPE         Latency type (podReadyLatency, schedulingLatency, etc.)
```

### container-stats

Statistical analysis of per-container time-series JSON files.

```bash
container-stats containerCPU.json
container-stats containerMemory.json cgroupMemoryRSS.json
```

### pod-latency-stats

Percentile band analysis of podLatencyMeasurement files.

```bash
pod-latency-stats podLatencyMeasurement-workload.json
pod-latency-stats --csv-only podLatencyMeasurement-workload.json
```

## Test

```bash
make test
```
