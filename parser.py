#!/usr/bin/env python3
from collections import Counter
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
    "max", "avg", "stddev", "end time", "percent", "duration", "cycles", "job_took"
]

COLUMN_ORDER = ORIGINAL_COLUMNS + QUANTILES_HEADERS

# --- 2. CONFIGURATION ---
METRICS_FILENAME = "podLatencyMeasurement-rds.json"
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

def print_visuals(metrics_list, frag, scheduler):
    if not plotille or not isinstance(metrics_list, list):
        return

    sorted_lats = sorted([i['schedulingLatency'] for i in metrics_list if 'schedulingLatency' in i])
    if not sorted_lats: return

    print(f"\n\033[1;34m" + "="*25 + f" VISUALS: {scheduler} {frag} " + "="*25 + "\033[0m")

    # --- 1. Scatterplot (Chronological Progress) ---
    data_points = []
    for i in metrics_list:
        if 'timestamp' in i and 'schedulingLatency' in i:
            try:
                ts = datetime.datetime.fromisoformat(i['timestamp'].replace('Z', '+00:00'))
                data_points.append((ts, i['schedulingLatency']))
            except: continue

    if data_points:
        data_points.sort(key=lambda x: x[0])
        start_t = data_points[0][0]
        x_secs = [(p[0] - start_t).total_seconds() for p in data_points]
        y_lats = [p[1] for p in data_points]
        print(f"\n[ Latency Scatterplot (Time vs. Delay) ]")
        latency_scatter = plotille.Figure()
        latency_scatter.set_x_limits(min_=0)
        latency_scatter.width, latency_scatter.height = 70, 12
        latency_scatter.scatter(x_secs, y_lats, lc='cyan')
        print(latency_scatter.show())

    # --- 2. Snap-to-Grid Histogram ---

    # 1. Determine Step
    lats_min, lats_max = min(sorted_lats), max(sorted_lats)
    step = get_pretty_step(lats_max - lats_min)

    # 2. SNAP DATA TO GRID
    # We transform the data into bucket labels (e.g., 2000, 4000, 6000)
    snapped_data = [math.floor(x / step) * step for x in sorted_lats]
    counts = Counter(snapped_data)

    # 3. Build a sorted list of buckets to display
    # This ensures we don't miss empty buckets in the middle
    start_bucket = math.floor(lats_min / step) * step
    end_bucket = math.floor(lats_max / step) * step

    print(f"\n[ Frequency Histogram ({step/1000:g}s Buckets) ]")
    print(f"{'Bucket Range (ms)':<18} | {'Chart':<40} | Count")
    max_count = max(counts.values()) if counts else 1
    curr = start_bucket
    while curr <= end_bucket:
        cnt = counts.get(curr, 0)
        # Create a simple ASCII bar based on percentage of max
        bar_len = int((cnt / max_count) * 40) if max_count > 0 else 0
        bar = "⣿" * bar_len

        print(f"[{curr:<7}, {curr+step:<7}) | {bar:<40} | {cnt}")
        curr += step

    # --- 3. CDF Line ---
    print("\n[ Cumulative Distribution (CDF) ]")
    n = len(sorted_lats)
    y_vals = [i / n for i in range(n)]
    cdf = plotille.Figure()
    cdf.set_x_limits(min_=0)
    cdf.set_y_limits(min_=0,max_=1)
    cdf.width, cdf.height = 70, 12
    cdf.plot(sorted_lats, y_vals)
    print(cdf.show())
    print("\033[1;34m" + "="*70 + "\033[0m\n")

def process_automation():
    uuid_fragments = sys.argv[1:]
    if not uuid_fragments:
        print(f"Usage: kb-parse <fragment1> <fragment2> ...")
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

        lat_path = os.path.join(pair['metrics_dir'], METRICS_FILENAME)

        try:
            with open(lat_path, 'r') as f:
                m_list = json.load(f)
                if isinstance(m_list, list):
                    lats = sorted([i['schedulingLatency'] for i in m_list if 'schedulingLatency' in i])
                    print_visuals(m_list, pair['fragment'], data['scheduler'])
                    data['stddev'] = round(statistics.stdev(lats), 2)
                    data['Spread'] = max(lats) - min(lats)
                    data['avg'] = round(statistics.mean(lats), 2)
                    data['max'] = max(lats)
                    data['CV'] = round(data['stddev'] / data['avg'], 3)
                    data['sched_p90'] = round(statistics.quantiles(lats, n=10)[8], 2)
                    dist = statistics.quantiles(lats, n=20)
                    for i, qh in enumerate(QUANTILES_HEADERS): data[qh] = round(dist[i], 2)
        except Exception as e: print(f"  [!] Metrics JSON Error: {e}")

        results.append({col: data.get(col, DEFAULT_VAL) for col in COLUMN_ORDER + ['Spread', 'CV']})

    # Summary Table
    print("\n" + " " * 20 + "\033[1;32m📊 FINAL COMPARISON SUMMARY\033[0m")
    print(f"{'Fragment':<12} | {'Scheduler':<15} | {'Replicas':<10} | {'Avg (ms)':<10} | {'Consistency (CV)':<15}")
    print("-" * 85)
    for r in results:
        cv = r.get('CV', 0)
        status = "✅ Stable" if (isinstance(cv, float) and cv < 0.2) else "❌ High Var"
        print(f"{str(r.get('UUID',''))[:8]:<12} | {r.get('scheduler',''):<15} | {r.get('podReplicas',''):<10} | {r.get('avg',''):<10} | {cv:<15} {status}")

    # CSV Dump
    with open(OUTPUT_FILE, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=COLUMN_ORDER, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)

    # Write to Standard Out (Console)
    print("\n" + "="*30 + " CSV RAW DATA " + "="*30)
    # We use sys.stdout as the 'file' for the writer
    console_writer = csv.DictWriter(sys.stdout, fieldnames=COLUMN_ORDER, extrasaction='ignore')
    console_writer.writeheader()
    console_writer.writerows(results)

if __name__ == "__main__":
    process_automation()
