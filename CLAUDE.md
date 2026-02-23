# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`kb-parser` is a Python CLI tool for parsing and visualizing performance metrics from **kube-burner-ocp** test runs. It analyzes Kubernetes pod scheduling latency to detect serial vs. parallel scheduling patterns, producing terminal visualizations and a CSV report.

## Setup and Running

```bash
# Install dependencies
pip install plotille==5.0.0 tqdm ijson msgpack orjson

# Make the script executable (if needed)
chmod +x parser.py
```

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
./parser.py 2178a534 metrics containerCPU --top-labels 20 -S
./parser.py 2178a534 metrics containerCPU -b ,0.05 --no-visuals
./parser.py 2178a534 metrics containerCPU -t 3621,6036 --no-visuals
```

Test data lives under directories matched by the fragment `2178a534` (collected-metrics and log file). Large test data files are excluded from Claude's context via `.claudeignore`.

## Architecture

The entire implementation lives in a single file: **`parser.py`**.

### Invocation

Positionals are classified by content, not position:
- `metrics` (literal) → enables metrics mode (latency does not run)
- any arg matching a real `.log` / `collected-metrics-*` entry → UUID fragment
- remaining args when `metrics` is present → metric file names

Classification is determined by running `find_pairs_recursively` on all non-`metrics` candidates; matched ones are UUID fragments, unmatched ones are metric file names. The caller guarantees no collision between the two sets. Recursively scans the current directory for `.log` files and `collected-metrics-<UUID>/` directories matching the given UUID fragments.

1. **Latency analysis:**  Extracts scheduling latency data from `podLatencyMeasurement-*.json` and job metadata from `jobSummary.json`.

2. **Metrics analysis (optional):** Loads each named metrics JSON file (array of `{metric: {labels}, value}` objects) from the discovered `collected-metrics-<UUID>/` directory, optionally filtering by `--label KEY=VALUE` pairs.

### Key Data Flow

1. File discovery (`find_pairs_recursively`) matches fragments to log/metrics pairs
2. Log parsing (`parse_logfmt_line`, `extract_log_metrics`) extracts scheduling stats and metadata
3. JSON metrics loaded and label-filtered (`load_generic_metrics`, `_load_lat_metrics`)
4. Statistics computed (percentiles P05–P95, mean, stdev, CV) — or loaded from cache
5. Terminal plots rendered via plotille (scatter, histogram, CDF)
6. Results written to `kube-burner-ocp-final-report.csv` and stdout

### Caching

Cache files sit alongside the source JSON. Format is `.kbcache.msgpack` when msgpack is available, falling back to `.kbcache.json`. Cache is invalidated when the source file's mtime changes.

- **In-memory cache** (`_mem_cache`): avoids re-reading the same file more than once per run.
- **Main values cache** (`_cache_path`): keyed on label filters only. Write-once on cold parse; never rewritten to append filter results. Stores sorted values, timestamps, and a `"stats"` sub-key with pre-computed mean/stdev/CV/percentiles.
- **Subset cache** (`_metrics_cache_path`, suffix `_sub_<hash>`): keyed on all active filter dimensions combined — label filters, value range (`-b`), and time range (`-t`). Stores the already-filtered values + stats so warm runs with filters skip loading the large main cache entirely.

**Known issue:** `-S` (cardinality / `need_labels=True`) currently bypasses the cache and triggers a cold ijson re-parse. Scheduled for fix in the caching simplification refactor.

### CSV Column Order

The output CSV has a strict fixed column ordering defined in `COLUMN_ORDER` and documented in `State.md`. New columns must be inserted at the correct position — do not append to the end arbitrarily.

### Coefficient of Variation (CV)

CV is used to classify scheduling parallelism patterns. Scheduling throughput (`max_pods_per_sec`, `avg_pods_per_sec`) is derived from timestamps in the latency measurement data.

### plotille Dependency

plotille is imported at runtime and the tool degrades gracefully if it is missing (prints a warning, skips visuals). `--no-visuals` also bypasses all plot rendering.

### Progress Indicators

All optional deps (plotille, tqdm, ijson, msgpack, orjson) degrade gracefully when absent.

- **`_progress(iterable, desc, unit)`**: wraps ijson streaming loops with a tqdm count+rate bar. No-ops when tqdm is missing, `--quiet` is set, or stderr is not a TTY.
- **`_spinner(desc, show_timer)`**: context manager for blocking non-iterating operations.
  - `show_timer=True` — background thread updates an elapsed counter every 1 s. Works for pure-Python ops where the GIL is released between bytecodes (e.g. plotille rendering, statistics computation).
  - `show_timer=False` — static label only, no thread. Used for C-extension ops (msgpack/json deserialisation) where the GIL is held and a frozen timer would be misleading.
- Graph output is buffered via `redirect_stdout(io.StringIO())` while the spinner is active to prevent the tqdm line and plot content from interleaving. The buffer's `isatty` is patched to mirror `sys.stdout.isatty` so plotille emits ANSI colour codes correctly.

### Planned Refactor

Latency entries (`podLatencyMeasurement-*.json`) and generic metrics entries share the same conceptual shape — `{timestamp, value}` pairs — but are loaded and processed through completely separate code paths (`process_automation` / `_load_lat_metrics` vs `run_generic_metrics_analysis` / `load_generic_metrics`). This causes duplicated filtering, stats, and plotting logic and means the `-t` time-range filter currently only works in metrics mode.

The intended fix is to normalise latency entries into the common `{timestamp, value}` format early in the pipeline so all downstream logic (time-clipping, stats, caching, plotting) can be shared. This refactor should happen alongside the caching simplification.
