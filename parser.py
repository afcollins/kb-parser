import json
import csv
import re
import glob
import os
import sys
import statistics

# --- CONFIGURATION ---
JSON_FILENAME = "podLatencyQuantilesMeasurement-rds.json"
OUTPUT_FILE = "kube_burner_results.csv"

def parse_logfmt_line(line):
    """Parses Golang logfmt style: key=value or key=\"value with spaces\""""
    pattern = r'(\w+)=(?:\"([^\"]*)\"|(\S+))'
    matches = re.findall(pattern, line)
    return {m[0]: (m[1] if m[1] else m[2]) for m in matches}

def extract_log_metrics(msg_content):
    """Extracts numeric values from the log's msg string."""
    metrics = {
        'p99': re.search(r"99th: (\d+)", msg_content),
        'max': re.search(r"max: (\d+)", msg_content),
        'avg': re.search(r"avg: (\d+)", msg_content)
    }
    return {k: (int(v.group(1)) if v else 0) for k, v in metrics.items()}

def process_automation():
    uuid_fragments = sys.argv[1:]
    if not uuid_fragments:
        print(f"Usage: python3 {sys.argv[0]} <uuid_fragment1> ...")
        return

    results = []

    for frag in uuid_fragments:
        print(f"--- Processing UUID Fragment: {frag} ---")

        # 1. LOG FILE: Always in the CURRENT directory
        log_pattern = f"*{frag}*.log"
        log_files = glob.glob(log_pattern)

        row_data = {
            'uuid': frag,
            'log_p99_ms': 0,
            'log_max_ms': 0,
            'log_avg_ms': 0,
            'sched_stddev': 0,
            'sched_p50': 0,
            'count': 0
        }

        if log_files:
            with open(log_files[0], 'r') as f:
                for line in f:
                    if "PodScheduled" in line:
                        parsed = parse_logfmt_line(line)
                        metrics = extract_log_metrics(parsed.get("msg", ""))
                        row_data.update({
                            'log_p99_ms': metrics['p99'],
                            'log_max_ms': metrics['max'],
                            'log_avg_ms': metrics['avg']
                        })
                        break

        # 2. JSON FILE: In the SUBDIRECTORY matching the fragment
        dir_matches = glob.glob(f"*{frag}*/")
        if dir_matches:
            target_dir = dir_matches[0]
            json_path = os.path.join(target_dir, JSON_FILENAME)

            if os.path.exists(json_path):
                with open(json_path, 'r') as f:
                    try:
                        data = json.load(f)
                        # Extract all schedulingLatency values from the array
                        latencies = [item['schedulingLatency'] for item in data if 'schedulingLatency' in item]

                        if len(latencies) > 1:
                            row_data['sched_stddev'] = round(statistics.stdev(latencies), 2)
                            # Ready for more percentiles:
                            row_data['sched_p50'] = statistics.median(latencies)
                            row_data['count'] = len(latencies)
                        elif len(latencies) == 1:
                            row_data['sched_p50'] = latencies[0]
                            row_data['count'] = 1

                    except (json.JSONDecodeError, KeyError, statistics.StatisticsError) as e:
                        print(f"  Note: Could not calculate stats for {json_path}: {e}")

        results.append(row_data)

    # 3. CSV EXPORT
    if results:
        with open(OUTPUT_FILE, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        print(f"\nSuccess! Processed {len(results)} rows into {OUTPUT_FILE}")

if __name__ == "__main__":
    process_automation()
