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
    # Convert range to seconds for easier logic
    range_sec = total_range_ms / 1000
    if range_sec <= 30: return 1000  # 1s
    if range_sec <= 90: return 2000  # 2s
    if range_sec <= 300: return 10000 # 10s
    return 30000 # 30s

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
    """Plot snap-to-grid frequency histogram of scheduling latencies."""
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


def _plot_cdf(sorted_lats):
    """Plot cumulative distribution of scheduling latencies."""
    print("\n[ Cumulative Distribution (CDF) ]")
    n = len(sorted_lats)
    y_vals = [i / n for i in range(n)]
    fig = plotille.Figure()
    fig.set_x_limits(min_=0)
    fig.set_y_limits(min_=0, max_=1)
    fig.width, fig.height = 70, 12
    fig.plot(sorted_lats, y_vals)
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
                    'workers': summary_data.get('otherNodesCount', 0)
                })
                job_cfg = summary_data.get('jobConfig', {})
                data['workload'] = job_cfg.get('name', DEFAULT_VAL)
                data['iterations'] = job_cfg.get('jobIterations', 0)
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
                        print_visuals(m_list, pair['fragment'], data['scheduler'])
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse kube-burner metrics and produce CSV reports.")
    parser.add_argument("--no-visuals", action="store_true",
                        help="Disable the large terminal plots (scatterplot, histogram, CDF) to save space.")
    parser.add_argument("fragments", nargs="*", help="UUID fragments to search for (e.g. from collected-metrics dirs).")
    args = parser.parse_args()

    if not args.fragments:
        parser.print_help()
        sys.exit(1)

    process_automation(args.fragments, no_visuals=args.no_visuals)
