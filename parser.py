#!/usr/bin/env python3
import argparse
from collections import Counter, defaultdict
import csv
import datetime
import hashlib
import json
import math
import os
import re
import statistics
import sys
import time

# Attempt to import plotille for terminal visuals
try:
    import plotille
except ImportError:
    print("[!] Run 'pip install plotille' to enable terminal graphing.")
    plotille = None

# --- 1. DYNAMIC COLUMN CONFIGURATION ---
# Percentiles we want to calculate
QUANTILES_HEADERS = [f"P{i:02d}" for i in range(5, 100, 5)]

# We define the order by finding 'job_took' and inserting the percentiles right after it
ORIGINAL_COLUMNS = [
    "OCP Version", "k-b version", "workers", "workload", "scheduler",
    "iterations", "podReplicas", "start time", "UUID", "p99",
    "max", "avg", "stddev", "end time", "percent", "duration", "cycles", "job_took",
    "Note", "worker reserved cores", "worker CPUs", "topologyPolicy", "overall duration",
    "qps burst", "CV"
]
EXPERIMENTAL_COLUMNS = [
    "max_pods_per_sec", "avg_pods_per_sec"
]

COLUMN_ORDER = ORIGINAL_COLUMNS + QUANTILES_HEADERS + EXPERIMENTAL_COLUMNS

# --- 2. CONFIGURATION ---
SUMMARY_FILENAME = "jobSummary.json"
OUTPUT_FILE = "kube-burner-ocp-final-report.csv"
DEFAULT_VAL = "-"


# --- 2. HELPER FUNCTIONS ---

def _parse_timestamp(ts_raw):
    """Parse ISO timestamp string, handling 'Z' suffix. Returns datetime or None."""
    try:
        return datetime.datetime.fromisoformat(str(ts_raw).replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


def get_pretty_step(total_range_ms):
    """Returns a clean step size (1s, 2s, 5s, 10s, etc.) based on total range."""
    range_sec = total_range_ms / 1000
    if range_sec <= 30: return 1000  # 1s
    if range_sec <= 90: return 2000  # 2s
    if range_sec <= 300: return 10000 # 10s
    return 30000 # 30s


def match_label_filters(entry, label_filters):
    """Return True if entry's labels match all label_filters (exact key=value)."""
    if not label_filters:
        return True
    labels = entry.get("labels") or {}
    return all(labels.get(k) == v for k, v in label_filters.items())


def _cache_path(filepath, label_filters=None):
    """Companion cache file path alongside source."""
    base = os.path.splitext(filepath)[0]
    if label_filters:
        key = hashlib.md5(str(sorted(label_filters.items())).encode()).hexdigest()[:8]
        return f"{base}_{key}.kbcache.json"
    return f"{base}.kbcache.json"

def _load_cache(cache_path, source_mtime):
    """Return cached dict (with 'values' or 'stats' keys) or None."""
    try:
        if not os.path.exists(cache_path):
            return None
        if os.path.getmtime(cache_path) < source_mtime:
            return None
        with open(cache_path) as f:
            data = json.load(f)
        if isinstance(data, list):          # upgrade old format
            return {"values": data}
        return data if isinstance(data, dict) else None
    except Exception:
        return None

def _save_cache(cache_path, data):
    try:
        with open(cache_path, 'w') as f:
            json.dump(data, f)
    except Exception:
        pass

def _get_cached_stats(cache_path, source_mtime):
    """Return pre-computed stats dict from cache if present, else None."""
    cached = _load_cache(cache_path, source_mtime)
    return cached.get("stats") if cached else None

def _save_stats_to_cache(cache_path, source_mtime, values_key, values, stats_dict):
    """Persist stats into the cache file."""
    try:
        cache_obj = _load_cache(cache_path, source_mtime) or {values_key: values}
        cache_obj["stats"] = stats_dict
        with open(cache_path, 'w') as f:
            json.dump(cache_obj, f)
    except Exception:
        pass


def load_generic_metrics(filepath, label_filters=None, return_entries=False):
    """
    Load a JSON list of metric objects (e.g. from collected-metrics), extract numeric
    'value' for each entry. If label_filters is a dict (e.g. {"id": "/kubepods.slice"}),
    only include entries whose labels match.

    Returns list of floats by default. When return_entries=True, returns the list of
    raw entry dicts (cache is bypassed for return value but still written for values).
    """
    cache_path = _cache_path(filepath, label_filters)
    source_mtime = os.path.getmtime(filepath)

    if not return_entries:
        _t0 = time.perf_counter()
        cached = _load_cache(cache_path, source_mtime)
        if cached is not None:
            _elapsed = time.perf_counter() - _t0
            print(f"  (cache loaded in {_elapsed:.1f}s)")
            return cached.get("values", [])

    with open(filepath, "r") as f:
        _t0 = time.perf_counter()
        data = json.load(f)
        _elapsed = time.perf_counter() - _t0
        print(f"  (raw loaded in {_elapsed:.1f}s)")
    if not isinstance(data, list):
        data = [data]
    entries = []
    values = []
    for entry in data:
        if "value" not in entry:
            continue
        if not match_label_filters(entry, label_filters):
            continue
        try:
            fval = float(entry["value"])
        except (TypeError, ValueError):
            continue
        values.append(fval)
        if return_entries:
            entries.append({**entry, "value": fval})
    _save_cache(cache_path, {"values": values})
    return entries if return_entries else values


def _load_lat_metrics(lat_path):
    """Load podLatencyMeasurement, caching [timestamp, schedulingLatency] pairs."""
    cache_path = _cache_path(lat_path)
    source_mtime = os.path.getmtime(lat_path)
    _t0 = time.perf_counter()
    cached = _load_cache(cache_path, source_mtime)
    if cached is not None:
        print(f"  (latency cache loaded in {time.perf_counter() - _t0:.1f}s)")
        return cached.get("entries", [])

    with open(lat_path, 'r') as f:
        m_list = json.load(f)
    if not isinstance(m_list, list):
        return []
    slim = [
        {"timestamp": i["timestamp"], "schedulingLatency": i["schedulingLatency"]}
        for i in m_list
        if "schedulingLatency" in i and "timestamp" in i
    ]
    _save_cache(cache_path, {"entries": slim})
    return slim


def parse_logfmt_line(line):
    pattern = r'(\w+)=(?:\"([^\"]*)\"|(\S+))'
    matches = re.findall(pattern, line)
    return {m[0]: (m[1] if m[1] else m[2]) for m in matches}

def extract_log_metrics(msg_content):
    metrics = {
        'p99': re.search(r"99th: (\d+)", msg_content),
        'max': re.search(r"max: (\d+)", msg_content),
        'avg': re.search(r"avg: (\d+)", msg_content)
    }
    return {k: (int(v.group(1)) if v else 0) for k, v in metrics.items()}

def find_pairs_recursively(fragments):
    pairs = []
    for root, dirs, files in os.walk('.'):
        for frag in fragments:
            metrics_dir_name = next((d for d in dirs if 'collected-metrics' in d and frag in d), None)
            log_match = next((f for f in files if frag in f and f.endswith(".log")), None)
            if metrics_dir_name or log_match:
                pairs.append({
                    'fragment': frag,
                    'metrics_dir': os.path.join(root, metrics_dir_name) if metrics_dir_name else None,
                    'log_path': os.path.join(root, log_match) if log_match else None,
                })
    return pairs

def compute_scheduling_throughput(metrics_list):
    """
    Compute scheduling throughput from podLatencyMeasurement entries.
    For each pod: scheduled_time = timestamp + schedulingLatency (ms).
    Returns (max_pods_per_sec, avg_pods_per_sec, counts_per_second).
    """
    second_counts = defaultdict(int)
    for i in metrics_list:
        if 'timestamp' not in i or 'schedulingLatency' not in i:
            continue
        ts = _parse_timestamp(i.get('timestamp'))
        if ts is None:
            continue
        try:
            lat_ms = int(i['schedulingLatency'])
            scheduled = ts + datetime.timedelta(milliseconds=lat_ms)
            key = scheduled.replace(microsecond=0)  # truncate to second
            second_counts[key] += 1
        except (ValueError, TypeError, KeyError):
            continue
    if not second_counts:
        return None, None, {}
    counts = list(second_counts.values())
    return max(counts), round(statistics.mean(counts), 2), dict(second_counts)


def _plot_latency_scatter(metrics_list):
    """Plot latency over time (seconds from start vs scheduling latency)."""
    data_points = []
    for i in metrics_list:
        if 'timestamp' in i and 'schedulingLatency' in i:
            ts = _parse_timestamp(i['timestamp'])
            if ts is None:
                print(f"  [!] Skipping scatter point: invalid timestamp {i.get('timestamp', '')!r}", file=sys.stderr)
                continue
            data_points.append((ts, i['schedulingLatency']))
    if not data_points:
        return
    data_points.sort(key=lambda x: x[0])
    start_t = data_points[0][0]
    x_secs = [(p[0] - start_t).total_seconds() for p in data_points]
    y_lats = [p[1] for p in data_points]
    print("\n[ Latency Scatterplot (Time vs. Delay) ]")
    fig = plotille.Figure()
    fig.set_x_limits(min_=0)
    fig.set_y_limits(min_=0)
    fig.width, fig.height = 70, 12
    fig.scatter(x_secs, y_lats, lc='cyan')
    print(fig.show())


def analyze_label_cardinality(entries, top_n=10):
    """
    Given a list of raw metric entry dicts (each with a 'labels' key),
    return {label_key: Counter({label_value: count, ...}), ...}.
    Also prints a human-readable summary, capped at top_n values per label key.
    """
    key_counters = defaultdict(Counter)
    for entry in entries:
        labels = entry.get("labels") or {}
        for k, v in labels.items():
            key_counters[k][v] += 1
    n = len(entries)
    print(f"  Labels across {n} entries:")
    for key in sorted(key_counters):
        counter = key_counters[key]
        total = len(counter)
        top = counter.most_common(top_n)
        parts = ", ".join(f"{v} ({c})" for v, c in top)
        suffix = f"  … +{total - top_n} more (see --top-labels)" if total > top_n else ""
        print(f"    {key:<12}: {parts}{suffix}")
    return dict(key_counters)


def _plot_metrics_scatter(entries, title_suffix="", t0=None):
    """Plot value over elapsed time for generic metric entries (timestamp + value).

    t0: optional datetime anchor for the X-axis origin. When provided (e.g. from
    the full unclipped dataset), elapsed seconds are computed relative to it so
    the X-axis position is preserved after time-range filtering.
    """
    data_points = []
    for entry in entries:
        ts_raw = entry.get("timestamp", "")
        val = entry.get("value")
        if not ts_raw or val is None:
            continue
        ts = _parse_timestamp(ts_raw)
        if ts is None:
            continue
        data_points.append((ts, float(val)))
    if not data_points:
        return
    data_points.sort(key=lambda x: x[0])
    start_t = t0 if t0 is not None else data_points[0][0]
    x_secs = [(p[0] - start_t).total_seconds() for p in data_points]
    y_vals = [p[1] for p in data_points]
    title = f"[ Metrics Scatter (Time vs. Value){(' ' + title_suffix) if title_suffix else ''} ]"
    print(f"\n{title}")
    fig = plotille.Figure()
    fig.set_x_limits(min_=min(x_secs), max_=max(x_secs))
    fig.set_y_limits(min_=0)
    fig.width, fig.height = 70, 12
    fig.scatter(x_secs, y_vals, lc='cyan')
    print(fig.show())


def _print_stats_table(title, n, avg, stdev, cv, p50, p90, p99,
                       min_val=None, max_val=None, unit="", extra_lines=None):
    """Print a standard stats summary block.

    min_val: shown when provided (metrics mode). Omitted for latency (run mode).
    unit: appended to numeric values, e.g. 'ms'.
    extra_lines: list of strings printed inside the box after the stat lines.
    """
    def fmt(v):
        return f"{v:.4g}" if isinstance(v, (int, float)) else str(v)
    u = unit
    print(f"\n\033[1;34m" + "=" * 50 + f" {title} " + "=" * 50 + "\033[0m")
    print(f"  N = {n}  |  avg = {fmt(avg)}{u}  |  stdev = {fmt(stdev)}  |  CV = {fmt(cv)}")
    parts = []
    if min_val is not None:
        parts.append(f"min = {fmt(min_val)}{u}")
    if max_val is not None:
        parts.append(f"max = {fmt(max_val)}{u}")
    parts += [f"P50 = {fmt(p50)}{u}", f"P90 = {fmt(p90)}{u}", f"P99 = {fmt(p99)}{u}"]
    print("  " + "  |  ".join(parts))
    for line in (extra_lines or []):
        print(f"  {line}")
    print("\033[1;34m" + "=" * 110 + "\033[0m")


def _plot_frequency_histogram(sorted_lats):
    """Plot snap-to-grid frequency histogram of scheduling latencies (clean ms boundaries)."""
    lats_min, lats_max = min(sorted_lats), max(sorted_lats)
    step = get_pretty_step(lats_max - lats_min)
    snapped_data = [math.floor(x / step) * step for x in sorted_lats]
    counts = Counter(snapped_data)
    start_bucket = math.floor(lats_min / step) * step
    end_bucket = math.floor(lats_max / step) * step
    print(f"\n[ Frequency Histogram ({step/1000:g}s Buckets) ]")
    print(f"{'Bucket Range (ms)':<18} | {'Chart':<40} | Count")
    max_count = max(counts.values()) if counts else 1
    curr = start_bucket
    while curr <= end_bucket:
        cnt = counts.get(curr, 0)
        bar_len = int((cnt / max_count) * 40) if max_count > 0 else 0
        bar = "⣿" * bar_len
        print(f"[{curr:<7}, {curr+step:<7}) | {bar:<40} | {cnt}")
        curr += step


def _plot_histogram_plotille(sorted_vals, title_suffix="", bins=20):
    """Plot frequency histogram using plotille (for generic metric values)."""
    if not sorted_vals:
        return
    title = f"[ Frequency Histogram {title_suffix} ]".strip()
    print(f"\n{title}")
    print(plotille.hist(sorted_vals, bins=bins))


def _plot_cdf(sorted_vals, title_suffix=""):
    """Plot cumulative distribution (CDF) for any sorted numeric values."""
    if not sorted_vals:
        return
    title = f"[ Cumulative Distribution (CDF) {title_suffix} ]".strip()
    print(f"\n{title}")
    n = len(sorted_vals)
    y_vals = [i / n for i in range(n)]
    fig = plotille.Figure()
    fig.set_x_limits(min_=0)
    fig.set_y_limits(min_=0, max_=1)
    fig.width, fig.height = 70, 12
    fig.plot(sorted_vals, y_vals)
    print(fig.show())


def clip_to_range(values, min_val=None, max_val=None):
    """Filter values to [min_val, max_val]. None means no bound."""
    if min_val is None and max_val is None:
        return values
    return [v for v in values
            if (min_val is None or v >= min_val)
            and (max_val is None or v <= max_val)]

def clip_entries_to_range(entries, min_val=None, max_val=None):
    """Filter entry dicts to those whose 'value' falls in [min_val, max_val]."""
    if min_val is None and max_val is None:
        return entries
    return [e for e in entries
            if (min_val is None or e["value"] >= min_val)
            and (max_val is None or e["value"] <= max_val)]


def clip_entries_to_time_range(entries, tmin_sec=None, tmax_sec=None):
    """Filter entry dicts to those whose timestamp falls within [tmin_sec, tmax_sec]
    elapsed seconds from the first timestamp in the dataset. None means no bound."""
    if tmin_sec is None and tmax_sec is None:
        return entries
    timestamps = []
    for e in entries:
        ts = _parse_timestamp(e.get("timestamp"))
        if ts is None:
            continue
        timestamps.append((ts, e))
    if not timestamps:
        return entries
    timestamps.sort(key=lambda x: x[0])
    start_t = timestamps[0][0]
    result = []
    for ts, e in timestamps:
        elapsed = (ts - start_t).total_seconds()
        if clip_to_range([elapsed], tmin_sec, tmax_sec):
            result.append(e)
    return result


def print_visuals(metrics_list, frag, scheduler, min_val=None, max_val=None):
    if not plotille or not isinstance(metrics_list, list):
        return
    sorted_lats = sorted([i['schedulingLatency'] for i in metrics_list if 'schedulingLatency' in i])
    if not sorted_lats:
        return

    plot_lats = clip_to_range(sorted_lats, min_val, max_val)
    if not plot_lats:
        print(f"  [!] No values in range [{min_val}, {max_val}] — skipping visuals.")
        return
    if len(plot_lats) < len(sorted_lats):
        print(f"  (showing {len(plot_lats)}/{len(sorted_lats)} values in [{min_val}, {max_val}]ms)")

    plot_metrics = metrics_list
    if min_val is not None or max_val is not None:
        plot_metrics = [i for i in metrics_list
                        if clip_to_range([i.get('schedulingLatency', 0)], min_val, max_val)]

    print(f"\n\033[1;34m" + "="*25 + f" VISUALS: {scheduler} {frag} " + "="*25 + "\033[0m")
    _plot_latency_scatter(plot_metrics)
    if min_val is not None or max_val is not None:
        _plot_histogram_plotille(plot_lats, 'Scheduling Latency Zoomed')
    else:
        _plot_frequency_histogram(plot_lats)
    _plot_cdf(plot_lats)
    print("\033[1;34m" + "="*70 + "\033[0m\n")

def process_automation(uuid_fragments, no_visuals=False, min_val=None, max_val=None):
    if not uuid_fragments:
        print(f"Usage: kb-parse [--no-visuals] <fragment1> <fragment2> ...")
        return

    discovered_pairs = find_pairs_recursively(uuid_fragments)
    if not discovered_pairs:
        print(f"No collocated pairs found for fragments: {uuid_fragments}")
        return

    results = []

    for pair in discovered_pairs:
        data = {'uuid_fragment': pair['fragment']}

        # Log Logic
        try:
            if not pair.get('log_path'):
                raise FileNotFoundError(f"no .log file found for fragment {pair['fragment']!r}")
            with open(pair['log_path'], 'r') as f:
                lines = f.readlines()
                if lines:
                    data['start time'] = parse_logfmt_line(lines[0]).get('time', DEFAULT_VAL)
                    data['end time'] = parse_logfmt_line(lines[-1]).get('time', DEFAULT_VAL)
                    for line in lines:
                        parsed = parse_logfmt_line(line)
                        msg = parsed.get('msg', '')
                        if "Starting kube-burner" in msg:
                            u_match = re.search(r"UUID ([a-f0-9\-]+)", msg)
                            v_match = re.search(r"\((.*?)\)", msg)
                            if u_match: data['UUID'] = u_match.group(1)
                            if v_match: data['k-b version'] = v_match.group(1).split('@')[0]
                        if "took" in msg and "Job" in msg:
                            d_match = re.search(r"took ([\w\.]+)", msg)
                            if d_match: data['job_took'] = d_match.group(1)
                        if "PodScheduled" in msg:
                            m = extract_log_metrics(msg)
                            data.update({'p99': m['p99'], 'max': m['max'], 'avg': m['avg']})
        except Exception as e:
            print(f"  [!] Log Error: {e}")

        # B. JSON Processing (only when a collected-metrics dir was found)
        if not pair.get('metrics_dir'):
            results.append({col: data.get(col, DEFAULT_VAL) for col in COLUMN_ORDER})
            continue
        summary_path = os.path.join(pair['metrics_dir'], SUMMARY_FILENAME)
        try:
            with open(summary_path, 'r') as f:
                summary_data = json.load(f)[0]
                data.update({
                    'OCP Version': summary_data.get('ocpVersion', DEFAULT_VAL),
                    'scheduler': summary_data.get('scheduler', DEFAULT_VAL),
                    'podReplicas': summary_data.get('podReplicas', DEFAULT_VAL),
                    'workers': summary_data.get('otherNodesCount', 0),
                    'worker reserved cores': summary_data.get('workerReservedCores', DEFAULT_VAL),
                    'worker CPUs': summary_data.get('workerCPUs', DEFAULT_VAL),
                    'topologyPolicy': summary_data.get('topologyPolicy', DEFAULT_VAL)
                })
                job_cfg = summary_data.get('jobConfig', {})
                data['workload'] = job_cfg.get('name', DEFAULT_VAL)
                data['iterations'] = job_cfg.get('jobIterations', 0)
                data['qps burst'] = job_cfg.get('qps', DEFAULT_VAL)
                churn = job_cfg.get('churnConfig', {})
                data.update({
                    'cycles': churn.get('cycles', DEFAULT_VAL),
                    'percent': churn.get('percent', DEFAULT_VAL),
                    'duration': f"{int(churn.get('duration', 0) / 60_000_000_000)}m"
                })
        except Exception as e: print(f"  [!] Summary JSON Error: {e}")

        lat_path = os.path.join(pair['metrics_dir'], f"podLatencyMeasurement-{data['workload']}.json")

        try:
            m_list = _load_lat_metrics(lat_path)
            if m_list:
                lats = sorted([i['schedulingLatency'] for i in m_list if 'schedulingLatency' in i])
                if not no_visuals:
                    _t0 = time.perf_counter()
                    print_visuals(m_list, pair['fragment'], data['scheduler'], min_val=min_val, max_val=max_val)
                    _elapsed = time.perf_counter() - _t0
                    if _elapsed > 0.5:
                        print(f"  (built graphs in {_elapsed:.1f}s)")
                cache_path = _cache_path(lat_path)
                source_mtime = os.path.getmtime(lat_path)
                cached_stats = _get_cached_stats(cache_path, source_mtime)
                if cached_stats:
                    data.update(cached_stats)
                    print(f"  (stats cache loaded)")
                else:
                    _t0 = time.perf_counter()
                    data['stddev'] = round(statistics.stdev(lats), 2)
                    data['avg'] = round(statistics.mean(lats), 2)
                    data['max'] = max(lats)
                    data['CV'] = round(data['stddev'] / data['avg'], 3)
                    data['sched_p90'] = round(statistics.quantiles(lats, n=10)[8], 2)
                    dist = statistics.quantiles(lats, n=20)
                    for i, qh in enumerate(QUANTILES_HEADERS): data[qh] = round(dist[i], 2)
                    # Scheduling throughput: scheduled_time = timestamp + schedulingLatency; count pods per second
                    max_pps, avg_pps, _ = compute_scheduling_throughput(m_list)
                    data['max_pods_per_sec'] = max_pps if max_pps is not None else DEFAULT_VAL
                    data['avg_pods_per_sec'] = avg_pps if avg_pps is not None else DEFAULT_VAL
                    _elapsed = time.perf_counter() - _t0
                    if _elapsed > 0.5:
                        print(f"  (crunched numbers in {_elapsed:.1f}s)")
                    stats_to_cache = {k: data[k]
                                      for k in (['stddev', 'avg', 'max', 'CV', 'sched_p90',
                                                  'max_pods_per_sec', 'avg_pods_per_sec'] + QUANTILES_HEADERS)
                                      if k in data}
                    _save_stats_to_cache(cache_path, source_mtime, "entries", m_list, stats_to_cache)

                _print_stats_table(
                    f"LATENCY STATS: {pair['fragment']}", len(lats),
                    data.get('avg', '-'), data.get('stddev', '-'), data.get('CV', '-'),
                    data.get('P50', '-'), data.get('P90', '-'), data.get('p99', '-'),
                    max_val=data.get('max', '-'), unit="ms",
                )
        except Exception as e: print(f"  [!] Metrics JSON Error: {e}")

        results.append({col: data.get(col, DEFAULT_VAL) for col in COLUMN_ORDER})

    # Summary Table — always printed
    print("\n" + " " * 20 + "\033[1;32m📊 FINAL COMPARISON SUMMARY\033[0m")
    print(f"{'Fragment':<12} | {'Scheduler':<15} | {'Replicas':<10} | {'Avg (ms)':<10} | {'Max pods/s':<10} | {'Avg pods/s':<10} | {'Consistency (CV)':<15}")
    print("-" * 110)
    for r in results:
        cv = r.get('CV', 0)
        status = _classify_cv(cv)
        print(f"{str(r.get('UUID',''))[:8]:<12} | {r.get('scheduler',''):<15} | {r.get('podReplicas',''):<10} | {r.get('avg',''):<10} | {r.get('max_pods_per_sec',''):<10} | {r.get('avg_pods_per_sec',''):<10} | {cv:<15} {status}")

    # CSV Dump (always write to file)
    with open(OUTPUT_FILE, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=COLUMN_ORDER, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)

    # Write to Standard Out (Console) — always printed
    print("\n" + "="*30 + " CSV RAW DATA " + "="*30)
    console_writer = csv.DictWriter(sys.stdout, fieldnames=COLUMN_ORDER, extrasaction='ignore')
    console_writer.writeheader()
    console_writer.writerows(results)


def run_generic_metrics_analysis(filepath, metric_name=None, label_filters=None,
                                  no_visuals=False, min_val=None, max_val=None,
                                  source=False, tmin_sec=None, tmax_sec=None,
                                  top_labels=10):
    """
    Run histogram, CDF, and CV (and summary stats) on a metrics JSON file that has
    objects with 'value' and optional 'labels'. Use label_filters to restrict
    (e.g. {"id": "/kubepods.slice", "node": "e26-h21-000-r650"}).
    """
    if not os.path.isfile(filepath):
        print(f"[!] Not a file: {filepath}", file=sys.stderr)
        return

    # Raw entries (with timestamps) are only needed for scatter plot (--source) or time filtering.
    # Histogram and CDF run from cached values alone, so the default path hits the cache.
    need_entries = source or (tmin_sec is not None or tmax_sec is not None)
    entries = None
    scatter_t0 = None  # X-axis anchor; set from full dataset when time-clipping
    if need_entries:
        entries = load_generic_metrics(filepath, label_filters=label_filters, return_entries=True)
        if tmin_sec is not None or tmax_sec is not None:
            # Compute t0 (minimum timestamp) from the full unclipped dataset so
            # the scatter X-axis preserves original elapsed seconds after clipping.
            parsed_ts = [ts for e in entries
                         if (ts := _parse_timestamp(e.get("timestamp"))) is not None]
            if parsed_ts:
                scatter_t0 = min(parsed_ts)
            original_n = len(entries)
            entries = clip_entries_to_time_range(entries, tmin_sec, tmax_sec)
            if not entries:
                print(f"  [!] No entries in time window [{tmin_sec}s, {tmax_sec}s]", file=sys.stderr)
                return
            print(f"  (time window [{tmin_sec}s, {tmax_sec}s]: {len(entries)}/{original_n} entries)")
        values = [e["value"] for e in entries]
    else:
        values = load_generic_metrics(filepath, label_filters=label_filters)

    if not values:
        print(f"[!] No values found in {filepath}" + (
            f" with label filters {label_filters}" if label_filters else ""
        ), file=sys.stderr)
        return

    display_name = metric_name or os.path.basename(filepath)
    title_suffix = f" — {display_name}" if display_name else ""
    if label_filters:
        title_suffix += " " + str(label_filters)

    sorted_vals = sorted(values)
    n = len(sorted_vals)

    cache_path = _cache_path(filepath, label_filters)
    source_mtime = os.path.getmtime(filepath)
    cached_stats = _get_cached_stats(cache_path, source_mtime)

    if cached_stats:
        avg   = cached_stats["avg"]
        stdev = cached_stats["stdev"]
        cv    = cached_stats["cv"]
        p50   = cached_stats["p50"]
        p90   = cached_stats["p90"]
        p99   = cached_stats["p99"]
    else:
        _t0 = time.perf_counter()
        avg   = statistics.mean(sorted_vals)
        stdev = statistics.stdev(sorted_vals) if n > 1 else 0.0
        cv    = round(stdev / avg, 3) if avg else 0.0
        p50   = statistics.median(sorted_vals)
        p90   = statistics.quantiles(sorted_vals, n=10)[8] if n >= 10 else sorted_vals[-1]
        p99   = statistics.quantiles(sorted_vals, n=100)[98] if n >= 100 else sorted_vals[-1]
        _elapsed = time.perf_counter() - _t0
        if _elapsed > 0.5:
            print(f"  (crunched numbers in {_elapsed:.1f}s)")
        _save_stats_to_cache(cache_path, source_mtime, "values", values,
                             {"avg": avg, "stdev": stdev, "cv": cv,
                              "p50": p50, "p90": p90, "p99": p99,
                              "min": min(sorted_vals), "max": max(sorted_vals)})

    extra = [f"Label filters: {label_filters}"] if label_filters else None
    _print_stats_table(
        f"METRICS: {display_name}", n, avg, stdev, cv, p50, p90, p99,
        min_val=min(sorted_vals), max_val=max(sorted_vals), extra_lines=extra,
    )

    # Show label cardinality when entries are already loaded (visuals, --source, or time filter).
    # Skipped on the cache-only path (--no-visuals without --source) to avoid forcing a raw load.
    if entries is not None:
        analyze_label_cardinality(clip_entries_to_range(entries, min_val, max_val), top_n=top_labels)

    if not no_visuals and plotille:
        plot_vals = clip_to_range(sorted_vals, min_val, max_val)
        if not plot_vals:
            print(f"  [!] No values in range [{min_val}, {max_val}]")
        else:
            if len(plot_vals) < len(sorted_vals):
                print(f"  (showing {len(plot_vals)}/{len(sorted_vals)} values in [{min_val}, {max_val}])")
            _t0 = time.perf_counter()
            if entries is not None:
                _plot_metrics_scatter(clip_entries_to_range(entries, min_val, max_val), title_suffix=title_suffix, t0=scatter_t0)
            _plot_histogram_plotille(plot_vals, title_suffix=title_suffix)
            _plot_cdf(plot_vals, title_suffix=title_suffix)
            _elapsed = time.perf_counter() - _t0
            if _elapsed > 0.5:
                print(f"  (built graphs in {_elapsed:.1f}s)")
    elif no_visuals:
        print("  (Use without --no-visuals to see histogram and CDF; add --source for scatter plot.)")



def _classify_cv(cv):
    """Return a human-readable scheduling pattern label for a CV value."""
    if not isinstance(cv, (int, float)):
        return "—"
    if cv >= 0.8:
        return "🔥 Bursty (optimal)"
    if cv >= 0.7:
        return "⚠️ Likely serial - check scatterplot"
    return "🐌 Serial scheduling rate"


def _parse_bucket_arg(raw_str, flag_name):
    """Parse 'MIN, MAX', ',MAX', or 'MIN' into (lo, hi). Exits on error."""
    raw = raw_str.strip().strip('[]()')
    try:
        if ',' in raw:
            left, right = raw.split(',', 1)
            return (float(left.strip()) if left.strip() else None,
                    float(right.strip()) if right.strip() else None)
        return float(raw), None
    except ValueError:
        print(f"[!] --{flag_name}: expected 'MIN, MAX', ',MAX', or 'MIN', "
              f"got: {raw_str!r}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse kube-burner metrics and produce CSV reports.",
        epilog="Run (default): %(prog)s [--no-visuals] fragment [fragment ...]  |  Metrics: %(prog)s metrics fragment [fragment ...] metric_file [--label KEY=VALUE ...] [--metric-name NAME] [--no-visuals]",
    )
    parser.add_argument("--no-visuals", action="store_true",
                        help="Disable the large terminal plots (scatterplot, histogram, CDF) to save space.")
    parser.add_argument("--metric-name", "-m", default=None, help="Display name for metric (metrics mode only; e.g. cgroupCPU).")
    parser.add_argument("--label", "-l", action="append", metavar="KEY=VALUE",
                        help="Filter by label in metrics mode (repeatable, e.g. -l id=/kubepods.slice).")
    parser.add_argument("--min", "-n", type=float, default=None, metavar="VALUE",
                        help="Only plot values >= VALUE (stats use full dataset).")
    parser.add_argument("--max", "-x", type=float, default=None, metavar="VALUE",
                        help="Only plot values <= VALUE (stats use full dataset).")
    parser.add_argument("--source", "-s", action="store_true",
                        help="In metrics mode: when a value range is active (--min/--max/--bucket), "
                             "show label distributions and timestamps for entries within that range.")
    parser.add_argument("--top-labels", type=int, default=10, metavar="N",
                        help="Max label values to show per key in cardinality output (default: 10).")
    parser.add_argument("--bucket", "-b", default=None, metavar='"MIN, MAX"',
                        help='Plot range from histogram bucket label, e.g. --bucket "12083200, 12096000". '
                             'Overrides --min/--max if both are given.')
    parser.add_argument("--tbucket", "-t", default=None, metavar='"START_SEC, END_SEC"',
                        help='Filter metrics to a time window by elapsed seconds from the scatter X-axis, '
                             'e.g. --tbucket "30, 90". Analogous to --bucket for values.')
    parser.add_argument("positionals", nargs="*",
                        help="Run: UUID fragments. Metrics: 'metrics' then fragments then metric file (e.g. metrics frag1 cgroupCPU.json).")

    args = parser.parse_args()
    if args.bucket is not None:
        args.min, args.max = _parse_bucket_arg(args.bucket, "bucket")
    if args.tbucket is not None:
        args.tmin, args.tmax = _parse_bucket_arg(args.tbucket, "tbucket")
    else:
        args.tmin = args.tmax = None
    positionals = args.positionals

    # Metrics mode: first positional is "metrics"
    if positionals and positionals[0] == "metrics":
        if len(positionals) < 3:
            print("metrics requires at least one fragment and a metric file (e.g. parser.py metrics frag1 cgroupCPU.json).", file=sys.stderr)
            sys.exit(1)
        fragments = positionals[1:-1]
        metric_file = positionals[-1]
        label_filters = {}
        if getattr(args, "label", None):
            for s in args.label:
                if "=" in s:
                    k, v = s.split("=", 1)
                    label_filters[k.strip()] = v.strip()
        discovered_pairs = find_pairs_recursively(fragments)
        if not discovered_pairs:
            print(f"No collected-metrics dirs found for fragments: {fragments}", file=sys.stderr)
            sys.exit(1)
        if not metric_file.endswith(".json"):
            metric_file = metric_file + ".json"
        display_base = getattr(args, "metric_name", None) or os.path.splitext(metric_file)[0]
        for pair in discovered_pairs:
            filepath = os.path.join(pair["metrics_dir"], metric_file)
            if not os.path.isfile(filepath):
                print(f"[!] Not found: {filepath}", file=sys.stderr)
                continue
            metric_name = display_base if len(discovered_pairs) == 1 else f"{display_base} ({pair['fragment']})"
            run_generic_metrics_analysis(
                filepath,
                metric_name=metric_name,
                label_filters=label_filters or None,
                no_visuals=args.no_visuals,
                min_val=args.min,
                max_val=args.max,
                source=args.source,
                tmin_sec=args.tmin,
                tmax_sec=args.tmax,
                top_labels=args.top_labels,
            )
        sys.exit(0)

    # Run mode (default): all positionals are UUID fragments
    fragments = positionals
    if not fragments:
        parser.print_help()
        sys.exit(1)
    process_automation(fragments, no_visuals=args.no_visuals, min_val=args.min, max_val=args.max)
