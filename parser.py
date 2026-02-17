import json
import csv
import re
import glob
import os
import sys
import statistics

# --- CONFIGURATION ---
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

def process_automation():
    uuid_fragments = sys.argv[1:]
    if not uuid_fragments:
        print(f"Usage: python3 {sys.argv[0]} <fragment1> <fragment2> ...")
        return

    results = []

    for frag in uuid_fragments:
        print(f"--- Processing: {frag} ---")

        row = {
            'full_uuid': 'N/A', 'version': 'N/A', 'start_time': 'N/A', 'end_time': 'N/A',
            'job_duration': 'N/A', 'ocpVersion': 'N/A', 'scheduler': 'N/A',
            'podReplicas': 0, 'jobIterations': 0, 'churn_cycles': 0,
            'churn_minutes': 0, 'churn_percent': 0, 'log_p99_ms': 0,
            'log_max_ms': 0, 'log_avg_ms': 0, 'sched_stddev': 0, 'sched_p50': 0
        }

        # 1. LOG PROCESSING (Current Directory)
        log_files = glob.glob(f"*{frag}*.log")
        if log_files:
            with open(log_files[0], 'r') as f:
                lines = f.readlines()
                if lines:
                    row['start_time'] = parse_logfmt_line(lines[0]).get('time', 'N/A')
                    row['end_time'] = parse_logfmt_line(lines[-1]).get('time', 'N/A')
                    for line in lines:
                        parsed = parse_logfmt_line(line)
                        msg = parsed.get('msg', '')
                        if "Starting kube-burner" in msg:
                            v_match = re.search(r"\((.*?)\)", msg)
                            u_match = re.search(r"UUID ([a-f0-9\-]+)", msg)
                            if v_match: row['version'] = v_match.group(1).split('@')[0]
                            if u_match: row['full_uuid'] = u_match.group(1)
                        if "took" in msg and "Job" in msg:
                            d_match = re.search(r"took ([\w\.]+)", msg)
                            if d_match: row['job_duration'] = d_match.group(1)
                        if "PodScheduled" in msg:
                            m = extract_log_metrics(msg)
                            row.update({'log_p99_ms': m['p99'], 'log_max_ms': m['max'], 'log_avg_ms': m['avg']})

        # 2. SUBDIRECTORY PROCESSING (JSON files)
        dir_matches = glob.glob(f"*{frag}*/")
        if dir_matches:
            target_dir = dir_matches[0]

            # --- Handle jobSummary.json ---
            summary_path = os.path.join(target_dir, SUMMARY_FILENAME)
            if os.path.exists(summary_path):
                with open(summary_path, 'r') as f:
                    try:
                        summary_data = json.load(f)[0] # It's a list
                        row['ocpVersion'] = summary_data.get('ocpVersion', 'N/A')
                        row['scheduler'] = summary_data.get('scheduler', 'N/A')
                        row['podReplicas'] = summary_data.get('podReplicas', 0)

                        job_cfg = summary_data.get('jobConfig', {})
                        row['jobIterations'] = job_cfg.get('jobIterations', 0)

                        churn = job_cfg.get('churnConfig', {})
                        row['churn_cycles'] = churn.get('cycles', 0)
                        row['churn_percent'] = churn.get('percent', 0)
                        # Convert nanoseconds to minutes
                        row['churn_minutes'] = churn.get('duration', 0) / 60_000_000_000
                    except Exception as e:
                        print(f"  Error parsing jobSummary: {e}")

            # --- Handle metadata.json (Latencies) ---
            metrics_path = os.path.join(target_dir, METRICS_FILENAME)
            if os.path.exists(metrics_path):
                with open(metrics_path, 'r') as f:
                    try:
                        m_data = json.load(f)
                        lats = [i['schedulingLatency'] for i in m_data if 'schedulingLatency' in i]
                        if len(lats) > 1:
                            row['sched_stddev'] = round(statistics.stdev(lats), 2)
                            row['sched_p50'] = statistics.median(lats)
                        elif lats:
                            row['sched_p50'] = lats[0]
                    except:
                        pass

        results.append(row)

    # 3. CSV EXPORT
    if results:
        with open(OUTPUT_FILE, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        print(f"\nReport ready for spreadsheet: {OUTPUT_FILE}")

if __name__ == "__main__":
    process_automation()
