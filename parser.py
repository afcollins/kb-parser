import json
import csv
import re
import glob
import os
import sys

def find_files_by_fragment(fragments, extension):
    """Finds all files in the current dir containing any of the fragments."""
    matched_files = []
    for frag in fragments:
        # Matches any file containing the fragment and ending with the extension
        search_pattern = f"*{frag}*{extension}"
        matched_file = glob.glob(search_pattern, recursive=True)
        print(f"Matched file: {matched_file}")
        matched_files.extend(matched_file)
    return list(set(matched_files))

def process_automation():
    # --- CLI ARGUMENT LOGIC ---
    # sys.argv[0] is the script name; sys.argv[1:] are the UUIDs you type
    uuid_fragments = sys.argv[1:]

    if not uuid_fragments:
        print("Usage: python3 parser.py <uuid_fragment1> <uuid_fragment2> ...")
        print("Example: python3 parser.py a1b2 889c")
        return

    results = []

    # 1. Process LOG Files
    log_files = find_files_by_fragment(uuid_fragments, ".log")
    for log_path in log_files:
        try:
            with open(log_path, 'r') as f:
                for line in f:
                    # Replace with your actual regex
                    match = re.search(r"Status: (\w+)", line)
                    if match:
                        results.append({
                            'uuid_ref': os.path.basename(log_path),
                            'type': 'Log Entry',
                            'data': match.group(1)
                        })
        except Exception as e:
            print(f"Error reading {log_path}: {e}")

    # 2. Process JSON Files + Calculation
    json_files = find_files_by_fragment(uuid_fragments, ".json")
    for json_path in json_files:
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
                
                # Calculation Logic
                # Example: Calculate a 15% tax or margin on a 'price' field
                raw_value = data.get('amount', 0)
                calculated_value = raw_value * 1.15 

                results.append({
                    'uuid_ref': os.path.basename(json_path),
                    'type': 'JSON Calc',
                    'data': round(calculated_value, 2)
                })
        except Exception as e:
            print(f"Error reading {json_path}: {e}")

    # 3. Save to CSV
    if not results:
        print(f"No files found matching fragments: {uuid_fragments}")
        return

    output_file = 'final_report.csv'
    with open(output_file, 'w', newline='') as f:
        # Uses the keys from the first result dictionary as headers
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"--- Process Complete ---")
    print(f"Files Scanned: {len(log_files) + len(json_files)}")
    print(f"Rows Created:  {len(results)}")
    print(f"Output saved to: {output_file}")

if __name__ == "__main__":
    process_automation()
