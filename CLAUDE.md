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

**Latency mode** (no `metrics` keyword):
```bash
./parser.py fragment1 fragment2
./parser.py --min 100 --max 5000 fragment1
./parser.py --bucket "12083200, 12096000" fragment1
./parser.py --no-visuals fragment1
```

**Metrics mode** (include `metrics` keyword; arg order is irrelevant):
```bash
./parser.py fragment1 metrics containerCPU
./parser.py fragment1 metrics containerCPU cgroupCPU
./parser.py metrics containerCPU fragment1 --label id=/kubepods.slice
./parser.py fragment1 metrics containerCPU -m "Display Name" --no-visuals
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
./parser.py 2178a534 metrics containerCPU --no-visuals
./parser.py 2178a534 metrics containerCPU
./parser.py 2178a534 metrics containerCPU --top-labels 20
```

Test data lives under directories matched by the fragment `2178a534` (collected-metrics and log file). Large test data files are excluded from Claude's context via `.claudeignore`.

## Architecture

The entire implementation lives in a single file: **`parser.py`**.

### Invocation

Positionals are classified by content, not position:
- `metrics` (literal) → enables metrics mode (latency does not run)
- any arg matching a real `.log` / `collected-metrics-*` entry → UUID fragment
- remaining args when `metrics` is present → metric file names

Classification is determined by running `find_pairs_recursively` on all non-`metrics` candidates; matched ones are UUID fragments, unmatched ones are metric file names. The caller guarantees no collision between the two sets.

1. **Latency analysis:** Recursively scans the current directory for `.log` files and `collected-metrics-<UUID>/` directories matching the given UUID fragments. Extracts scheduling latency data from `podLatencyMeasurement-*.json` and job metadata from `jobSummary.json`.

2. **Metrics analysis (optional):** Loads each named metrics JSON file (array of `{metric: {labels}, value}` objects) from the discovered `collected-metrics-<UUID>/` directory, optionally filtering by `--label KEY=VALUE` pairs.

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
