import json
import csv
import re
import glob
import os
import sys

# --- CONFIGURATION ---
JSON_FILENAME = "podLatencyQuantilesMeasurement-rds.json"
OUTPUT_FILE = "kube_metrics_report.csv"

def parse_logfmt_line(line):
    """Parses Golang logfmt style: key=value or key=\"value with spaces\""""
    pattern = r'(\w+)=(?:\"([^\"]*)\"|(\S+))'
    matches = re.findall(pattern, line)
    return {m[0]: (m[1] if m[1] else m[2]) for m in matches}

def extract_metrics(msg_content):
    """Extracts numeric values from the msg string."""
    # Pattern looks for 'label: value ms'
    metrics = {
        'p99': re.search(r"99th: (\d+)", msg_content),
        'max': re.search(r"max: (\d+)", msg_content),
        'avg': re.search(r"avg: (\d+)", msg_content)
    }
    # Return integer value if found, else None
    return {k: (int(v.group(1)) if v else None) for k, v in metrics.items()}

def process_automation():
    uuid_fragments = sys.argv[1:]
    if not uuid_fragments:
        print(f"Usage: python3 {sys.argv[0]} <uuid_fragment1> ...")
        return

    results = []

    for frag in uuid_fragments:
        # Find directory matching fragment
        dir_matches = glob.glob(f"*{frag}*/")
        if not dir_matches:
            print(f"Skipping: No folder found for {frag}")
            continue

        target_dir = dir_matches[0]

        # 1. Process LOG file inside the directory
        log_files = glob.glob(f"*{frag}*.log")

        row_data = {
            'uuid': frag,
            'p99_ms': None,
            'max_ms': None,
            'avg_ms': None,
            'json_calc': 0
        }

        if log_files:
            with open(log_files[0], 'r') as f:
                for line in f:
                    # We only care about lines with PodScheduled metrics
                    if "PodScheduled" in line:
                        parsed_line = parse_logfmt_line(line)
                        msg = parsed_line.get("msg", "")
                        print(msg)
                        metrics = extract_metrics(msg)
                        row_data.update({
                            'p99_ms': metrics['p99'],
                            'max_ms': metrics['max'],
                            'avg_ms': metrics['avg']
                        })
                        break # Stop after finding the first matching metric line

        # 2. Process JSON file (Calculation Placeholder)
        json_path = os.path.join(target_dir, JSON_FILENAME)
        if os.path.exists(json_path):
            with open(json_path, 'r') as f:
                try:
                    data = json.load(f)
                    # Placeholder: replace 'raw_value' with your actual JSON key
                    row_data['json_calc'] = data.get('raw_value', 0) * 1.0
                except:
                    pass

        results.append(row_data)

    # 3. Save to CSV
    if results:
        with open(OUTPUT_FILE, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        print(f"Success! Created {OUTPUT_FILE} with {len(results)} rows.")

if __name__ == "__main__":
    process_automation()
