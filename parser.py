#!/usr/bin/env python3
import argparse
from collections import Counter, defaultdict
import csv
import datetime
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


def load_generic_metrics(filepath, label_filters=None):
    """
    Load a JSON list of metric objects (e.g. from collected-metrics), extract numeric
    'value' for each entry. If label_filters is a dict (e.g. {"id": "/kubepods.slice"}),
    only include entries whose labels match. Returns list of floats.
    """
    with open(filepath, "r") as f:
        data = json.load(f)
    if not isinstance(data, list):
        data = [data]
    values = []
    for entry in data:
        if "value" not in entry:
            continue
        if not match_label_filters(entry, label_filters):
            continue
        try:
            values.append(float(entry["value"]))
        except (TypeError, ValueError):
            continue
    return values


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
            if metrics_dir_name:
                log_match = next((f for f in files if frag in f and f.endswith(".log")), None)
                if log_match:
                    pairs.append({
                        'fragment': frag,
                        'log_path': os.path.join(root, log_match),
                        'metrics_dir': os.path.join(root, metrics_dir_name)
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
        try:
            ts = datetime.datetime.fromisoformat(i['timestamp'].replace('Z', '+00:00'))
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
            # Guard: metrics may have non-ISO or malformed timestamps (e.g. empty, wrong format).
            try:
                ts = datetime.datetime.fromisoformat(i['timestamp'].replace('Z', '+00:00'))
                data_points.append((ts, i['schedulingLatency']))
            except (ValueError, TypeError) as e:
                print(f"  [!] Skipping scatter point: invalid timestamp {i.get('timestamp', '')!r}: {e}", file=sys.stderr)
                continue
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


def print_visuals(metrics_list, frag, scheduler):
    if not plotille or not isinstance(metrics_list, list):
        return
    sorted_lats = sorted([i['schedulingLatency'] for i in metrics_list if 'schedulingLatency' in i])
    if not sorted_lats:
        return

    print(f"\n\033[1;34m" + "="*25 + f" VISUALS: {scheduler} {frag} " + "="*25 + "\033[0m")
    _plot_latency_scatter(metrics_list)
    _plot_frequency_histogram(sorted_lats)
    _plot_cdf(sorted_lats)
    print("\033[1;34m" + "="*70 + "\033[0m\n")

def process_automation(uuid_fragments, no_visuals=False):
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

        # B. JSON Processing
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
            with open(lat_path, 'r') as f:
                m_list = json.load(f)
                if isinstance(m_list, list):
                    lats = sorted([i['schedulingLatency'] for i in m_list if 'schedulingLatency' in i])
                    if not no_visuals:
                        _t0 = time.perf_counter()
                        print_visuals(m_list, pair['fragment'], data['scheduler'])
                        _elapsed = time.perf_counter() - _t0
                        if _elapsed > 0.5:
                            print(f"  (built graphs in {_elapsed:.1f}s)")
                    _t0 = time.perf_counter()
                    data['stddev'] = round(statistics.stdev(lats), 2)
                    data['Spread'] = max(lats) - min(lats)
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
        except Exception as e: print(f"  [!] Metrics JSON Error: {e}")

        results.append({col: data.get(col, DEFAULT_VAL) for col in COLUMN_ORDER + ['Spread', 'CV']})

    # Summary Table — always printed
    print("\n" + " " * 20 + "\033[1;32m📊 FINAL COMPARISON SUMMARY\033[0m")
    print(f"{'Fragment':<12} | {'Scheduler':<15} | {'Replicas':<10} | {'Avg (ms)':<10} | {'Max pods/s':<10} | {'Avg pods/s':<10} | {'Consistency (CV)':<15}")
    print("-" * 110)
    for r in results:
        cv = r.get('CV', 0)
        if isinstance(cv, (int, float)):
            if cv >= 0.8:
                status = "🔥 Bursty (optimal)"
            elif cv >= 0.7:
                status = "⚠️ Likely serial - check scatterplot"
            else:
                status = "🐌 Serial scheduling rate"
        else:
            status = "—"
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


def run_generic_metrics_analysis(filepath, metric_name=None, label_filters=None, no_visuals=False):
    """
    Run histogram, CDF, and CV (and summary stats) on a metrics JSON file that has
    objects with 'value' and optional 'labels'. Use label_filters to restrict
    (e.g. {"id": "/kubepods.slice", "node": "e26-h21-000-r650"}).
    """
    if not os.path.isfile(filepath):
        print(f"[!] Not a file: {filepath}", file=sys.stderr)
        return
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
    _t0 = time.perf_counter()
    avg = statistics.mean(sorted_vals)
    stdev = statistics.stdev(sorted_vals) if n > 1 else 0.0
    cv = round(stdev / avg, 3) if avg else 0.0
    p50 = statistics.median(sorted_vals)
    p90 = statistics.quantiles(sorted_vals, n=10)[8] if n >= 10 else sorted_vals[-1]
    p99 = statistics.quantiles(sorted_vals, n=100)[98] if n >= 100 else sorted_vals[-1]
    _elapsed = time.perf_counter() - _t0
    if _elapsed > 0.5:
        print(f"  (crunched numbers in {_elapsed:.1f}s)")

    print("\n\033[1;34m" + "=" * 50 + f" METRICS: {display_name} " + "=" * 50 + "\033[0m")
    print(f"  N = {n}  |  avg = {avg:.4g}  |  stdev = {stdev:.4g}  |  CV = {cv}")
    print(f"  min = {min(sorted_vals):.4g}  |  max = {max(sorted_vals):.4g}  |  P50 = {p50:.4g}  |  P90 = {p90:.4g}  |  P99 = {p99:.4g}")
    if label_filters:
        print(f"  Label filters: {label_filters}")
    print("\033[1;34m" + "=" * 110 + "\033[0m")

    if not no_visuals and plotille:
        _t0 = time.perf_counter()
        _plot_histogram_plotille(sorted_vals, title_suffix=title_suffix)
        _plot_cdf(sorted_vals, title_suffix=title_suffix)
        _elapsed = time.perf_counter() - _t0
        if _elapsed > 0.5:
            print(f"  (built graphs in {_elapsed:.1f}s)")
    elif no_visuals:
        print("  (Use without --no-visuals to see histogram and CDF.)")


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
    parser.add_argument("positionals", nargs="*",
                        help="Run: UUID fragments. Metrics: 'metrics' then fragments then metric file (e.g. metrics frag1 cgroupCPU.json).")

    args = parser.parse_args()
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
            )
        sys.exit(0)

    # Run mode (default): all positionals are UUID fragments
    fragments = positionals
    if not fragments:
        parser.print_help()
        sys.exit(1)
    process_automation(fragments, no_visuals=args.no_visuals)
