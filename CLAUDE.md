# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`kb-parser` is a Python CLI tool for parsing and visualizing performance metrics from **kube-burner-ocp** test runs. It analyzes Kubernetes pod scheduling latency to detect serial vs. parallel scheduling patterns, producing terminal visualizations and a CSV report.

## Setup and Running

```bash
# Install the only external dependency
pip install plotille==5.0.0

# Make the script executable (if needed)
chmod +x parser.py
```

**Run mode** (process kube-burner results by UUID fragments):
```bash
./parser.py fragment1 fragment2
./parser.py --min 100 --max 5000 fragment1
./parser.py --bucket "12083200, 12096000" fragment1
./parser.py --no-visuals fragment1
```

**Metrics mode** (analyze a generic metrics JSON file):
```bash
./parser.py metrics fragment1 cgroupCPU.json
./parser.py metrics fragment1 cgroupCPU.json --label id=/kubepods.slice
./parser.py metrics fragment1 cgroupCPU.json -m "Display Name" --no-visuals
```

There are no automated tests. The `sample-cgroupCPU.json` file serves as example input for metrics mode.

## Test Commands

Use these to manually verify both modes against real data in the repo:

**Run mode:**
```bash
./parser.py 2178a534 --no-visuals
./parser.py 2178a534
```

**Metrics mode:**
```bash
./parser.py metrics 2178a534 containerCPU.json --no-visuals
./parser.py metrics 2178a534 containerCPU.json
./parser.py metrics 2178a534 containerCPU.json --top-labels 20
```

Test data lives under directories matched by the fragment `2178a534` (collected-metrics and log file). Large test data files are excluded from Claude's context via `.claudeignore`.

## Architecture

The entire implementation lives in a single file: **`parser.py`**.

### Two Modes

1. **Run mode (default):** Recursively scans the current directory for `.log` files and `collected-metrics-<UUID>/` directories matching the given UUID fragments. Extracts scheduling latency data from `podLatencyMeasurement-*.json` and job metadata from `jobSummary.json`.

2. **Metrics mode:** Loads a generic metrics JSON file (array of `{metric: {labels}, values: [[timestamp, value]]}` objects), optionally filtering by `--label KEY=VALUE` pairs.

### Key Data Flow

1. File discovery (`find_pairs_recursively`) matches fragments to log/metrics pairs
2. Log parsing (`parse_logfmt_line`, `extract_log_metrics`) extracts scheduling stats and metadata
3. JSON metrics loaded and label-filtered (`load_generic_metrics`, `_load_lat_metrics`)
4. Statistics computed (percentiles P05–P95, mean, stdev, CV) — or loaded from cache
5. Terminal plots rendered via plotille (scatter, histogram, CDF)
6. Results written to `kube-burner-ocp-final-report.csv` and stdout

### Caching

Large JSON files are cached in `.kbcache.json` companion files (sibling to the source). Cache keys use an MD5 hash of label filters; cache is invalidated when the source file's mtime changes. Both raw values and pre-computed statistics are stored.

### CSV Column Order

The output CSV has a strict fixed column ordering defined in `COLUMN_ORDER` and documented in `State.md`. New columns must be inserted at the correct position — do not append to the end arbitrarily.

### Coefficient of Variation (CV)

CV is used to classify scheduling parallelism patterns. Scheduling throughput (`max_pods_per_sec`, `avg_pods_per_sec`) is derived from timestamps in the latency measurement data.

### plotille Dependency

plotille is imported at runtime and the tool degrades gracefully if it is missing (prints a warning, skips visuals). `--no-visuals` also bypasses all plot rendering.
