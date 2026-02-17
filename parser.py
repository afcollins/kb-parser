import json
import csv
import re
import glob
import os
import sys

# --- CONFIGURATION ---
# Change this to the exact name of your JSON file
JSON_FILENAME = "podLatencyQuantilesMeasurement-rds.json"


def parse_golang_log(line):
    # This regex captures: key=value OR key="value with spaces"
    # Group 1: key, Group 2: value (handling quotes)
    pattern = r'(\w+)=(?:\"([^\"]*)\"|(\S+))'
    matches = re.findall(pattern, line)

    # Convert matches to a dictionary: {'level': 'info', 'msg': '...', ...}
    log_dict = {m[0]: (m[1] if m[1] else m[2]) for m in matches}
    return log_dict

def process_automation():
    # Capture UUID fragments from command line
    uuid_fragments = sys.argv[1:]

    if not uuid_fragments:
        print(f"Usage: python3 {sys.argv[0]} <uuid1> <uuid2> ...")
        return

    results = []

    for frag in uuid_fragments:
        print(f"--- Searching for fragment: {frag} ---")

        # 1. Find the Directory matching the fragment
        # This looks for any directory in the current folder containing the fragment
        dir_matches = glob.glob(f"*{frag}*/")

        if not dir_matches:
            print(f"Skipping: No directory found for {frag}")
            continue

        target_dir = dir_matches[0] # Take the first match

        # 2. Find the LOG file inside that specific directory
        # Uses the fragment to identify the log file
        log_files = glob.glob(f"*{frag}*.log")

        log_val = "N/A"
        p99_val = "n/a"
        max_val = "n/a"
        avg_val = "n/a"

        if log_files:
            with open(log_files[0], 'r') as f:
                print(f"Searching {f}")
                for line in f:
                    # REPLACE: Your grep logic
                    # Example: searching for 'throughput: 500'
                    if "PodScheduled" in line:
                        log_val = line.split("PodScheduled")[1].strip()
                        log_line_split = log_val.split(" ")
                        p99_val = log_line_split[1]
                        max_val = log_line_split[4]
                        avg_val = log_line_split[7]
                        print(log_line_split)
                        break

        # 3. Find the Static JSON file inside that specific directory
#        json_path = os.path.join(target_dir, JSON_FILENAME)
#        calc_result = 0
#
#        if os.path.exists(json_path):
#            with open(json_path, 'r') as f:
#                try:
#                    data = json.load(f)
#                    # REPLACE: Your calculation logic
#                    # Example: data['raw_metric'] * 2
#                    raw_data = data.get('raw_metric', 0)
#                    calc_result = raw_data * 1.5
#                except json.JSONDecodeError:
#                    print(f"Error: Invalid JSON in {json_path}")

        # 4. Save the row
        results.append({
            'uuid_fragment': frag,
            'p99': p99_val,
            'max': max_val,
            'avg': avg_val,
            'json_calc': '0.0',
            'folder': target_dir
        })

    # --- CSV EXPORT ---
    if not results:
        return

    output_file = 'processed_results.csv'
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSuccess! Exported {len(results)} rows to {output_file}")

if __name__ == "__main__":
    process_automation()
