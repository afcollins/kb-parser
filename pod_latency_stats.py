#!/usr/bin/env python3
"""
Statistical analysis of podLatencyMeasurement JSON files from kube-burner cluster-density-v2 runs.

Detects bimodal distributions, identifies optimal split points, recommends
percentiles for change detection, and outputs a CSV for cross-run comparison.

Usage:
    python3 pod_latency_stats.py podLatencyMeasurement-cluster-density-v2.json
    python3 pod_latency_stats.py run1/podLatency*.json run2/podLatency*.json
    python3 pod_latency_stats.py --csv-only podLatency*.json   # CSV output only, no report
"""
import csv
import json
import os
import statistics
import sys
from collections import defaultdict

LATENCY_FIELDS = [
    'schedulingLatency', 'initializedLatency', 'containersReadyLatency',
    'podReadyLatency', 'containersStartedLatency', 'readyToStartContainersLatency'
]

RECOMMENDED_PERCENTILES = [1, 2, 5, 10, 20, 25, 33, 50, 51, 65, 68, 69, 75, 80, 88, 90, 95, 98, 99]


def compute_percentile(sorted_vals, p):
    """Return the value at the p-th percentile (0-100) from a sorted list."""
    n = len(sorted_vals)
    if n == 0:
        return 0
    idx = min(int(n * p / 100), n - 1)
    return sorted_vals[idx]


def find_optimal_split(sorted_vals):
    """Find the split point that minimises within-group variance (Otsu-style)."""
    n = len(sorted_vals)
    unique_sorted = sorted(set(sorted_vals))
    if len(unique_sorted) < 4:
        return None

    best_split = None
    best_score = float('inf')

    for i in range(1, len(unique_sorted)):
        threshold = (unique_sorted[i - 1] + unique_sorted[i]) / 2
        low = [v for v in sorted_vals if v <= threshold]
        high = [v for v in sorted_vals if v > threshold]
        if len(low) < 10 or len(high) < 10:
            continue
        var_low = statistics.pvariance(low) if len(low) > 1 else 0
        var_high = statistics.pvariance(high) if len(high) > 1 else 0
        weighted_var = (len(low) * var_low + len(high) * var_high) / n
        if weighted_var < best_score:
            best_score = weighted_var
            best_split = threshold

    return best_split


def analyze_field(vals_sorted, field_name):
    """Return a dict of stats and bimodal analysis for one latency field."""
    n = len(vals_sorted)
    result = {
        'field': field_name,
        'n': n,
        'mean': statistics.mean(vals_sorted),
        'std': statistics.pstdev(vals_sorted),
        'min': vals_sorted[0],
        'max': vals_sorted[-1],
        'unique_values': len(set(vals_sorted)),
    }
    result['cv'] = (result['std'] / result['mean'] * 100) if result['mean'] > 0 else 0

    for p in RECOMMENDED_PERCENTILES:
        result[f'P{p}'] = compute_percentile(vals_sorted, p)

    # Bimodal split
    split = find_optimal_split(vals_sorted)
    result['split_point'] = split
    if split is not None:
        low = sorted([v for v in vals_sorted if v <= split])
        high = sorted([v for v in vals_sorted if v > split])
        n_low, n_high = len(low), len(high)
        result['band1_n'] = n_low
        result['band1_pct'] = n_low / n * 100
        result['band1_mean'] = statistics.mean(low)
        result['band1_std'] = statistics.pstdev(low)
        result['band1_min'] = low[0]
        result['band1_max'] = low[-1]
        result['band1_cv'] = (result['band1_std'] / result['band1_mean'] * 100) if result['band1_mean'] > 0 else 0
        result['band2_n'] = n_high
        result['band2_pct'] = n_high / n * 100
        result['band2_mean'] = statistics.mean(high)
        result['band2_std'] = statistics.pstdev(high)
        result['band2_min'] = high[0]
        result['band2_max'] = high[-1]
        result['band2_cv'] = (result['band2_std'] / result['band2_mean'] * 100) if result['band2_mean'] > 0 else 0
        result['gap'] = high[0] - low[-1]
        avg_std = (result['band1_std'] + result['band2_std']) / 2
        result['separation_quality'] = result['gap'] / avg_std if avg_std > 0 else float('inf')

        # Per-band percentiles
        for p in RECOMMENDED_PERCENTILES:
            result[f'band1_P{p}'] = compute_percentile(low, p)
            result[f'band2_P{p}'] = compute_percentile(high, p)

        # CDF transition detection
        percentiles = {p: compute_percentile(vals_sorted, p) for p in range(1, 100)}
        deltas = {}
        for p in range(2, 99):
            deltas[p] = percentiles[p + 1] - percentiles[p - 1]
        sorted_deltas = sorted(deltas.items(), key=lambda x: x[1], reverse=True)
        for rank, (p, d) in enumerate(sorted_deltas[:5], 1):
            result[f'transition_{rank}_percentile'] = p
            result[f'transition_{rank}_delta_ms'] = d
            result[f'transition_{rank}_value_ms'] = percentiles[p]
    else:
        for key in ['band1_n', 'band1_pct', 'band1_mean', 'band1_std', 'band1_min',
                     'band1_max', 'band1_cv', 'band2_n', 'band2_pct', 'band2_mean',
                     'band2_std', 'band2_min', 'band2_max', 'band2_cv', 'gap',
                     'separation_quality']:
            result[key] = ''
        for p in RECOMMENDED_PERCENTILES:
            result[f'band1_P{p}'] = ''
            result[f'band2_P{p}'] = ''
        for rank in range(1, 6):
            result[f'transition_{rank}_percentile'] = ''
            result[f'transition_{rank}_delta_ms'] = ''
            result[f'transition_{rank}_value_ms'] = ''

    return result


def analyze_file(filepath):
    """Analyze one podLatencyMeasurement JSON file. Returns (metadata, list of field results)."""
    with open(filepath) as f:
        data = json.load(f)

    # Extract run metadata from first record
    first = data[0]
    metadata = {
        'file': os.path.basename(filepath),
        'filepath': filepath,
        'uuid': first.get('uuid', ''),
        'jobName': first.get('jobName', ''),
        'ocp_version': first.get('metadata', {}).get('ocpVersion', ''),
        'total_records': len(data),
    }

    field_results = []
    for field in LATENCY_FIELDS:
        vals = sorted(float(entry[field]) for entry in data)
        result = analyze_field(vals, field)
        result.update(metadata)
        field_results.append(result)

    return metadata, field_results


def print_report(metadata, field_results):
    """Print a human-readable report to stdout."""
    print(f"\n{'#' * 110}")
    print(f"# {metadata['file']}  (uuid={metadata['uuid']}  ocp={metadata['ocp_version']})")
    print(f"# Records: {metadata['total_records']}")
    print(f"{'#' * 110}")

    for r in field_results:
        print(f"\n{'=' * 100}")
        print(f"  {r['field']}:")
        print(f"    N={r['n']}  Mean={r['mean']:.1f}  Std={r['std']:.1f}  CV={r['cv']:.1f}%  "
              f"Unique={r['unique_values']}")
        print(f"    Min={r['min']:.0f}  P5={r['P5']:.0f}  P25={r['P25']:.0f}  "
              f"Median={r['P50']:.0f}  P75={r['P75']:.0f}  P95={r['P95']:.0f}  "
              f"P99={r['P99']:.0f}  Max={r['max']:.0f}")

        if r['split_point'] is not None and r['split_point'] != '':
            print(f"\n    Bimodal split at {r['split_point']:.0f} ms  "
                  f"(separation quality: {r['separation_quality']:.2f})")
            print(f"    Band 1 (fast): N={r['band1_n']} ({r['band1_pct']:.1f}%)  "
                  f"Mean={r['band1_mean']:.1f}  Std={r['band1_std']:.1f}  "
                  f"Range=[{r['band1_min']:.0f}, {r['band1_max']:.0f}]  CV={r['band1_cv']:.1f}%")
            print(f"    Band 2 (slow): N={r['band2_n']} ({r['band2_pct']:.1f}%)  "
                  f"Mean={r['band2_mean']:.1f}  Std={r['band2_std']:.1f}  "
                  f"Range=[{r['band2_min']:.0f}, {r['band2_max']:.0f}]  CV={r['band2_cv']:.1f}%")
            print(f"    Gap: {r['gap']:.0f} ms")

            print(f"\n    Top CDF transitions:")
            for rank in range(1, 6):
                tp = r.get(f'transition_{rank}_percentile', '')
                if tp != '':
                    print(f"      P{tp}: delta={r[f'transition_{rank}_delta_ms']:.0f}ms  "
                          f"value={r[f'transition_{rank}_value_ms']:.0f}ms")

            print(f"\n    Recommended percentiles:")
            plist = [('P20', 'band center'), ('P51', 'band center'),
                     ('P68', 'transition'), ('P69', 'transition'),
                     ('P75', 'band center'), ('P88', 'band center'),
                     ('P98', 'band tail')]
            for pname, role in plist:
                v = r.get(pname, '')
                if v != '':
                    print(f"      {pname} = {v:.0f} ms  ({role})")

    # Value distribution for key fields
    print(f"\n{'=' * 100}")
    print("VALUE DISTRIBUTIONS")
    print(f"{'=' * 100}")
    # Re-read for distribution (we only have the sorted vals in results)
    # Just show the unique-value fields from results
    for r in field_results:
        if r['unique_values'] <= 20:
            print(f"\n  {r['field']}: {r['unique_values']} unique values (see CSV for full data)")


def write_csv(all_results, csv_path):
    """Write all field results across all files to a single CSV."""
    if not all_results:
        return

    # Build stable column order
    # Metadata first, then global stats, then percentiles, then bimodal, then transitions
    meta_cols = ['file', 'uuid', 'jobName', 'ocp_version', 'total_records']
    stat_cols = ['field', 'n', 'mean', 'std', 'cv', 'min', 'max', 'unique_values']
    pct_cols = [f'P{p}' for p in RECOMMENDED_PERCENTILES]
    split_cols = ['split_point', 'gap', 'separation_quality',
                  'band1_n', 'band1_pct', 'band1_mean', 'band1_std',
                  'band1_min', 'band1_max', 'band1_cv',
                  'band2_n', 'band2_pct', 'band2_mean', 'band2_std',
                  'band2_min', 'band2_max', 'band2_cv']
    band_pct_cols = []
    for p in RECOMMENDED_PERCENTILES:
        band_pct_cols.append(f'band1_P{p}')
        band_pct_cols.append(f'band2_P{p}')
    trans_cols = []
    for rank in range(1, 6):
        trans_cols.extend([f'transition_{rank}_percentile',
                           f'transition_{rank}_delta_ms',
                           f'transition_{rank}_value_ms'])

    columns = meta_cols + stat_cols + pct_cols + split_cols + band_pct_cols + trans_cols

    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        for r in all_results:
            writer.writerow(r)

    print(f"\nCSV written to: {csv_path}")


def main():
    csv_only = False
    filepaths = []
    for arg in sys.argv[1:]:
        if arg == '--csv-only':
            csv_only = True
        else:
            filepaths.append(arg)

    if not filepaths:
        print(f"Usage: {sys.argv[0]} [--csv-only] <file.json> [file2.json ...]")
        print("  Analyzes podLatencyMeasurement JSON files and outputs report + CSV.")
        sys.exit(1)

    all_results = []
    for filepath in filepaths:
        metadata, field_results = analyze_file(filepath)
        all_results.extend(field_results)
        if not csv_only:
            print_report(metadata, field_results)

    # Write CSV next to the first input file
    first_dir = os.path.dirname(os.path.abspath(filepaths[0]))
    csv_path = os.path.join(first_dir, 'podLatency-percentile-bands.csv')
    write_csv(all_results, csv_path)


if __name__ == '__main__':
    main()
