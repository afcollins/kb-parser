# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`kb-parser` is a Python CLI tool for parsing and visualizing performance metrics from **kube-burner-ocp** test runs. It analyzes Kubernetes pod scheduling latency to detect serial vs. parallel scheduling patterns, producing terminal visualizations and a CSV report.

## Setup and Running

```bash
pip install plotille==5.0.0 tqdm ijson msgpack orjson
chmod +x parser.py
```

## Test Commands

```bash
# Latency mode
./parser.py 2178a534 --no-visuals
./parser.py 2178a534 -t 0,10 --no-visuals
./parser.py 2178a534 -b ,3000 --no-visuals
./parser.py 2178a534 -S --no-visuals

# Metrics mode
./parser.py 2178a534 metrics containerCPU --no-visuals
./parser.py 2178a534 metrics containerCPU --top-labels 20 -S
./parser.py 2178a534 metrics containerCPU -b ,0.05 --no-visuals
./parser.py 2178a534 metrics containerCPU -t 3621,6036 --no-visuals
```

Test data lives under directories matched by the fragment `2178a534`. Large test data files are excluded from Claude's context via `.claudeignore`.

## Architecture

Single file: **`parser.py`**.

### Invocation

Positionals are classified by content, not position: `metrics` (literal) enables metrics mode; any arg matching a real `.log` / `collected-metrics-*` entry is a UUID fragment; remaining args (when `metrics` is present) are metric file names.

### Key Data Flow

1. File discovery (`find_pairs_recursively`) matches fragments to log/metrics pairs
2. Log parsing (`parse_logfmt_line`, `extract_log_metrics`) extracts scheduling stats and metadata
3. JSON loaded and filtered (`load_generic_metrics`, `_load_lat_metrics_normalized`)
4. Statistics computed via `_compute_stats()` (percentiles P05–P95, mean, stdev, CV) — or loaded from cache
5. Terminal plots rendered via plotille (scatter, histogram, CDF)
6. Results written to `kube-burner-ocp-final-report.csv` and stdout

### Unified Latency / Metrics Pipeline

Both latency and generic metrics entries share the normalized format `{"timestamp": epoch_float, "value": float, "labels": dict}`, enabling shared filtering, stats, caching, and plotting. `field_filters` are converted to label filters at load time. `-t`, `-b`, and `-S` all work in both modes.

### Caching

Cache files sit alongside the source JSON (`.kbcache.msgpack` or `.kbcache.json`), invalidated by source mtime.

- **In-memory cache** (`_mem_cache`): avoids re-reading the same file more than once per run.
- **Main values cache** (`_metrics_cache_path`): one function covers all paths. Generic metrics keyed on label filters; latency keyed on `latency_key` (each type is an independent cache, like a separate metric file). Stores sorted values, timestamps, labels, and pre-computed stats.
- **Subset cache** (`_metrics_cache_path`, suffix `_sub_<hash>`): keyed on all active filter dimensions — label filters, latency key, value range (`-b`), time range (`-t`).

### CSV Column Order

Fixed ordering defined in `COLUMN_ORDER`. New columns must be inserted at the correct position.

### plotille and Optional Dependencies

All optional deps (plotille, tqdm, ijson, msgpack, orjson) degrade gracefully when absent. Graph output is buffered via `redirect_stdout(io.StringIO())` while spinners are active; the buffer's `isatty` is patched so plotille emits ANSI colours correctly.
