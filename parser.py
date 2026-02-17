#!/usr/bin/env python3
import json
import csv
import re
import glob
import os
import sys
import statistics

# --- 1. COLUMN ORDER CONFIGURATION ---
# Rearrange these strings to change the order in your CSV/Spreadsheet
COLUMNS_START = [
    "OCP Version", "k-b version", "workers", "workload", "scheduler",
    "iterations", "podReplicas", "start time", "UUID", "p99",
    "max", "avg", "stddev", "end time", "percent", "duration", "cycles", "job_took", "sched_p90"
]
QUANTILES = [f"P{i:02d}" for i in range(5, 100, 5)]
COLUMN_ORDER = COLUMNS_START + QUANTILES

# --- 2. FILE NAMES ---
METRICS_FILENAME = "podLatencyMeasurement-rds.json"
SUMMARY_FILENAME = "jobSummary.json"
OUTPUT_FILE = "kube-burner-ocp-final-report.csv"

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

def process_automation():
    uuid_fragments = sys.argv[1:]
    if not uuid_fragments:
        print(f"Usage: python3 {sys.argv[0]} <fragment1> ...")
        return

    results = []

    for frag in uuid_fragments:
        print(f"--- Processing: {frag} ---")

        # Internal storage using a dictionary
        data = {}

        # 1. LOG PROCESSING (Current Directory)
        log_files = glob.glob(f"*{frag}*.log")
        if log_files:
            with open(log_files[0], 'r') as f:
                lines = f.readlines()
                if lines:
                    data['start time'] = parse_logfmt_line(lines[0]).get('time', 'N/A')
                    data['end time'] = parse_logfmt_line(lines[-1]).get('time', 'N/A')
                    for line in lines:
                        parsed = parse_logfmt_line(line)
                        msg = parsed.get('msg', '')
                        if "Starting kube-burner" in msg:
                            v_match = re.search(r"\((.*?)\)", msg)
                            u_match = re.search(r"UUID ([a-f0-9\-]+)", msg)
                            if v_match: data['k-b version'] = v_match.group(1).split('@')[0]
                            if u_match: data['UUID'] = u_match.group(1)
                        if "PodScheduled" in msg:
                            m = extract_log_metrics(msg)
                            data.update({'p99': m['p99'], 'max': m['max'], 'avg': m['avg']})
                        if "took" in msg and "Job" in msg:
                            d_match = re.search(r"took ([\w\.]+)", msg)
                            if d_match:
                                # Changed key from 'job_duration' to 'job_took' to match the list above
                                data['job_took'] = d_match.group(1)


        # 2. SUBDIRECTORY PROCESSING (JSON files)
        dir_matches = glob.glob(f"*{frag}*/")
        if dir_matches:
            target_dir = dir_matches[0]

            # --- Handle jobSummary.json ---
            summary_path = os.path.join(target_dir, SUMMARY_FILENAME)
            if os.path.exists(summary_path):
                with open(summary_path, 'r') as f:
                    try:
                        summary_data = json.load(f)[0]
                        data['OCP Version'] = summary_data.get('ocpVersion', '-')
                        data['scheduler'] = summary_data.get('scheduler', '-')
                        data['podReplicas'] = summary_data.get('podReplicas', '-')
                        data['workers'] = summary_data.get('otherNodesCount', 0)

                        job_cfg = summary_data.get('jobConfig', {})
                        data['workload'] = job_cfg.get('name', '-')
                        data['iterations'] = job_cfg.get('jobIterations', 0)

                        churn = job_cfg.get('churnConfig', {})
                        data['cycles'] = churn.get('cycles', '-')
                        data['percent'] = churn.get('percent', '-')
                        raw_dur = (churn.get('duration', '-') or 0)
                        data['duration'] = f"{int(raw_dur / 60_000_000_000)}m"
                    except: pass

            # --- Handle metadata.json (Latencies) ---
            metrics_path = os.path.join(target_dir, METRICS_FILENAME)
            if os.path.exists(metrics_path):
                with open(metrics_path, 'r') as f:
                    try:
                        m_list = json.load(f)
                        lats = [i['schedulingLatency'] for i in m_list if 'schedulingLatency' in i]
                        if len(lats) > 1:
                            data['stddev'] = round(statistics.stdev(lats), 2)
                            # n=10 splits data into 10 groups, the 9th index is the 90th percentile
                            data['sched_p90'] = statistics.quantiles(lats, n=10)[8]

                            # n=20 gives us 5% intervals (100/5 = 20)
                            # This returns a list of 19 values (P05, P10 ... P95)
                            dist_values = statistics.quantiles(lats, n=20)

                            # Map the calculated values to our dynamic headers
                            for i, q_header in enumerate(QUANTILES):
                                data[q_header] = round(dist_values[i], 2)
                        else:
                            data['stddev'] = 0
                            for qh in QUANTILES_HEADERS: data[qh] = 0
                    except FileNotFoundError:
                            print(f"Warning: Summary file missing for {frag}")
                    except json.JSONDecodeError:
                            print(f"Error: {summary_path} contains invalid JSON")

        # Ensure all columns exist in the row even if missing from files
        row = {col: data.get(col, '-') for col in COLUMN_ORDER}
        results.append(row)

    # 3. CSV EXPORT
    if results:
        with open(OUTPUT_FILE, 'w', newline='') as f:
            # The 'extrasaction' parameter prevents crashes if a key is missing
            writer = csv.DictWriter(f, fieldnames=COLUMN_ORDER, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(results)

        # Write to Standard Out (Console)
        print(f"\n--- CSV DATA START ---\n")
        # We use sys.stdout as the 'file' for the writer
        console_writer = csv.DictWriter(sys.stdout, fieldnames=COLUMN_ORDER, extrasaction='ignore')
        console_writer.writeheader()
        console_writer.writerows(results)
        print(f"\n--- CSV DATA END ---\n")

        print(f"\nReport ready: {OUTPUT_FILE}")

if __name__ == "__main__":
    process_automation()
