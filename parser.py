#!/usr/bin/env python3
import json
import csv
import re
import glob
import os
import sys
import statistics

# Attempt to import plotille for terminal visuals
try:
    import plotille
except ImportError:
    print("[!] Run 'pip install plotille' to enable terminal graphing.")
    plotille = None

# --- 1. DYNAMIC COLUMN CONFIGURATION ---
BASE_START = ["OCP Version", "k-b version", "workers", "workload", "scheduler", "iterations", "podReplicas", "start time", "UUID"]
BASE_END = ["p99", "max", "avg", "stddev", "end time", "job_took", "percent", "duration", "cycles"]
QUANTILES_HEADERS = [f"P{i:02d}" for i in range(5, 100, 5)]
COLUMN_ORDER = BASE_START + BASE_END + QUANTILES_HEADERS

# --- 2. CONFIGURATION ---
METRICS_FILENAME = "podLatencyMeasurement-rds.json"
SUMMARY_FILENAME = "jobSummary.json"
OUTPUT_FILE = "kube_burner_final_report.csv"

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
    """
    Searches subdirectories to find pairs of:
    1. A folder named 'collected-metrics'
    2. A .log file containing the UUID fragment
    They must be in the same parent directory.
    """
    pairs = []
    # os.walk('.') starts at your current folder and goes as deep as needed
    for root, dirs, files in os.walk('.'):
        # 1. Check if 'collected-metrics' exists in this current 'root'
        if 'collected-metrics' in dirs:
            # 2. Look for a .log file in this same 'root' folder that matches our fragments
            for frag in fragments:
                log_match = next((f for f in files if frag in f and f.endswith(".log")), None)

                if log_match:
                    metrics_path = os.path.join(root, 'collected-metrics')
                    log_path = os.path.join(root, log_match)
                    pairs.append({
                        'fragment': frag,
                        'log_path': log_path,
                        'metrics_dir': metrics_path
                    })
                    print(f"Found match: {root}")
    return pairs

def print_visuals(lats, frag):
    if not plotille or not lats:
        return

    print(f"\n\033[1;34m" + "="*20 + f" VISUALS FOR {frag} " + "="*20 + "\033[0m")

    # Histogram: Shows Frequency/Bottlenecks
    print("\n[ Frequency Histogram ]")
    print(plotille.hist(lats, bins=20, width=70))

    # CDF: Shows Progression/S-Curve
    print("\n[ Cumulative Distribution (CDF) ]")
    sorted_lats = sorted(lats)
    n = len(sorted_lats)
    y_values = [i / n for i in range(n)]
    fig = plotille.Figure()
    fig.width, fig.height = 70, 15
    fig.plot(sorted_lats, y_values)
    print(fig.show())
    print("\033[1;34m" + "="*70 + "\033[0m\n")

def process_automation():
    uuid_fragments = sys.argv[1:]
    if not uuid_fragments:
        print(f"Usage: kb-parse <fragment1> <fragment2> ...")
        return

    # Find all collocated pairs across the whole directory tree
    discovered_pairs = find_pairs_recursively(uuid_fragments)
    results = []

    for pair in discovered_pairs:
        frag = pair['fragment']
        data = {'uuid_fragment': frag}

        # --- LOG PROCESSING ---
        log_files = glob.glob(f"*{frag}*.log")
        if log_files:
            try:
                with open(log_files[0], 'r') as f:
                    lines = f.readlines()
                if lines:
                    data['start time'] = parse_logfmt_line(lines[0]).get('time', 'N/A')
                    data['end time'] = parse_logfmt_line(lines[-1]).get('time', 'N/A')
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
                print(f"Error reading log {pair['log_path']}: {e}")

        # --- 2. Process the JSONs (Inside pair['metrics_dir']) ---
        # The metrics dir is the 'collected-metrics' folder found earlier
        metrics_dir = pair['metrics_dir']
        dir_matches = glob.glob(f"*{frag}*/")
        if dir_matches:
            target_dir = dir_matches[0]

            # 1. jobSummary
            summary_path = os.path.join(metrics_dir, "jobSummary.json")
            try:
                with open(summary_path, 'r') as f:
                    summary_data = json.load(f)[0]
                    data.update({
                        'OCP Version': summary_data.get('ocpVersion', 'N/A'),
                        'scheduler': summary_data.get('scheduler', 'N/A'),
                        'podReplicas': summary_data.get('podReplicas', 0),
                        'workers': summary_data.get('otherNodesCount', 0)
                    })
                    job_cfg = summary_data.get('jobConfig', {})
                    data['workload'] = job_cfg.get('name', 'N/A')
                    data['iterations'] = job_cfg.get('jobIterations', 0)
                    churn = job_cfg.get('churnConfig', {})
                    data.update({
                        'cycles': churn.get('cycles', 0),
                        'percent': churn.get('percent', 0),
                        'duration': f"{int(churn.get('duration', 0) / 60_000_000_000)}m"
                    })
            except Exception as e: print(f"[!] Error Summary JSON: {e}")

            # 2. metadata (Latencies)
            metrics_path = os.path.join(metrics_dir, "podLatencyMeasurement-rds.json")
            try:
                with open(metrics_path, 'r') as f:
                    m_list = json.load(f)
                    lats = [i['schedulingLatency'] for i in m_list if 'schedulingLatency' in i]
                    if lats:
                        print_visuals(lats, frag)
                        data['stddev'] = round(statistics.stdev(lats), 2) if len(lats) > 1 else 0
                        data['Spread'] = max(lats) - min(lats)
                        data['CV'] = round((data['stddev'] / (sum(lats)/len(lats))), 3) if sum(lats) > 0 else 0
                        dist = statistics.quantiles(lats, n=20)
                        for i, qh in enumerate(QUANTILES_HEADERS): data[qh] = round(dist[i], 2)
            except Exception as e: print(f"[!] Error Metrics JSON: {e}")

        results.append({col: data.get(col, 'N/A') for col in COLUMN_ORDER + ['Spread', 'CV']})

    # --- FINAL SUMMARY TABLE ---
    print("\n" + " " * 20 + "\033[1;32m📊 FINAL COMPARISON SUMMARY\033[0m")
    print(f"{'Fragment':<12} | {'Scheduler':<15} | {'Avg (ms)':<10} | {'Spread':<10} | {'Consistency (CV)':<15}")
    print("-" * 75)
    for r in results:
        avg = r.get('avg', 0)
        cv = r.get('CV', 0)
        status = "✅ Stable" if cv < 0.2 else "⚠️ Noisy" if cv < 0.5 else "❌ High Variance"
        print(f"{r['UUID'][:8]:<12} | {r['scheduler']:<15} | {avg:<10} | {r['Spread']:<10} | {cv:<15} {status}")

    # --- CSV SAVE ---
    with open(OUTPUT_FILE, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=COLUMN_ORDER, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)
    print(f"\nFull report saved to {OUTPUT_FILE}")

# --- FINAL SUMMARY TABLE ---
    print("\n" + " " * 20 + "\033[1;32m📊 FINAL COMPARISON SUMMARY\033[0m")
    print(f"{'Fragment':<12} | {'Scheduler':<15} | {'Avg (ms)':<10} | {'Spread':<10} | {'Consistency (CV)':<15}")
    print("-" * 85)

    total_avg = []

    for r in results:
        avg = r.get('avg', 0)
        cv = r.get('CV')

        # Add to global list for the final row if it's a valid number
        if isinstance(avg, (int, float)) and avg > 0:
            total_avg.append(avg)

        # Safe type checking for the status labels
        if isinstance(cv, (int, float)):
            status = "✅ Stable" if cv < 0.2 else "⚠️ Noisy" if cv < 0.5 else "❌ High Variance"
            cv_display = f"{cv:<15.3f}"
        else:
            status = "❔ Missing Data"
            cv_display = f"{'N/A':<15}"

        # Handle UUID vs Fragment display
        label = r['UUID'][:8] if r.get('UUID') != 'N/A' else r.get('uuid_fragment', 'Unknown')

        print(f"{label:<12} | {r['scheduler']:<15} | {avg:<10} | {r.get('Spread', 'N/A'):<10} | {cv_display} {status}")

    # --- GLOBAL AVERAGE FOOTER ---
    if total_avg:
        global_mean = round(sum(total_avg) / len(total_avg), 2)
        print("-" * 85)
        print(f"{'BATCH TOTAL':<12} | {'N/A':<15} | {global_mean:<10} | {'N/A':<10} | {'N/A':<15} (Avg of all runs)")

if __name__ == "__main__":
    process_automation()
