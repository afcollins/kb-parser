#!/usr/bin/env python3
"""
Statistical analysis of OpenShift metric JSON files with label-grouped time-series.

Supports any file with {timestamp, labels, value} records:
  containerCPU.json, containerMemory.json, cgroupCPU.json, cgroupMemoryRSS.json,
  nodeCPU-*.json, nodeMemoryUtilization-*.json, crioCPU.json, kubeletMemory.json, etc.

Auto-detects label keys from the data, groups by each label dimension,
computes per-group statistics (mean, std, CV, percentiles), clusters labels by
magnitude and variability, and detects anomalies (spike ratios, top consumers).

Usage:
    python3 container_stats.py containerCPU.json
    python3 container_stats.py cgroupCPU.json cgroupMemoryRSS.json
    python3 container_stats.py nodeCPU-Workers.json nodeMemoryUtilization-Workers.json
"""
import argparse
import csv
import json
from pathlib import Path
from version import __version__
import sys
import statistics
from collections import defaultdict


def detect_metric_type(data):
    """Detect whether values are CPU cores or memory bytes."""
    sample_values = [float(entry['value']) for entry in data[:100]]
    avg = statistics.mean(sample_values)
    metric_name = data[0].get('metricName', '')
    if 'memory' in metric_name.lower() or 'Memory' in metric_name or avg > 10000:
        return 'memory'
    return 'cpu'


def fmt_val(value, metric_type):
    """Format a value for display based on metric type."""
    if metric_type == 'memory':
        return f"{value:.3e}"
    return f"{value:.4f}"


def fmt_val_header(metric_type):
    """Column width for formatted values."""
    if metric_type == 'memory':
        return 12
    return 10


def compute_stats(values):
    n = len(values)
    if n == 0:
        return None
    s = sorted(values)
    mean = statistics.mean(s)
    std = statistics.pstdev(s) if n > 1 else 0
    cv = (std / mean * 100) if mean > 0 else 0
    mn, mx = s[0], s[-1]
    median = statistics.median(s)
    p5 = s[int(n * 0.05)] if n >= 20 else s[0]
    p25 = s[int(n * 0.25)]
    p75 = s[int(n * 0.75)] if n > 1 else s[0]
    p95 = s[int(n * 0.95)] if n >= 20 else s[-1]
    p99 = s[int(n * 0.99)] if n >= 100 else s[-1]
    iqr = p75 - p25
    return {
        'n': n, 'mean': mean, 'std': std, 'cv': cv,
        'min': mn, 'max': mx, 'median': median,
        'p5': p5, 'p25': p25, 'p75': p75, 'p95': p95, 'p99': p99,
        'iqr': iqr
    }


def get_magnitude_bands(metric_type):
    if metric_type == 'memory':
        return [
            (0, 1e6, "Near-zero (<1 MB)"),
            (1e6, 1e7, "Very low (1-10 MB)"),
            (1e7, 1e8, "Low (10-100 MB)"),
            (1e8, 1e9, "Moderate (100 MB - 1 GB)"),
            (1e9, 1e10, "High (1-10 GB)"),
            (1e10, float('inf'), "Very high (10+ GB)")
        ]
    return [
        (0, 0.001, "Near-zero (<0.001 cores)"),
        (0.001, 0.01, "Very low (0.001-0.01 cores)"),
        (0.01, 0.1, "Low (0.01-0.1 cores)"),
        (0.1, 1.0, "Moderate (0.1-1.0 cores)"),
        (1.0, 10.0, "High (1-10 cores)"),
        (10.0, float('inf'), "Very high (10+ cores)")
    ]


CV_BANDS = [
    (0, 15, "Very stable (CV<15%)"),
    (15, 30, "Stable (CV 15-30%)"),
    (30, 60, "Moderate variability (CV 30-60%)"),
    (60, 100, "High variability (CV 60-100%)"),
    (100, float('inf'), "Very high variability (CV>100%)")
]


def get_min_mean_threshold(metric_type):
    """Minimum mean for 'most predictable' filter."""
    if metric_type == 'memory':
        return 1e6  # 1 MB
    return 0.01


def _open_input(filepath):
    """Return (data, display_name) from a file path or '-' for stdin."""
    if filepath == '-':
        return json.load(sys.stdin), '<stdin>'
    with open(filepath) as f:
        return json.load(f), filepath


def _validate_metric_records(data):
    """Validate the generic {timestamp, labels, value} input format."""
    if not isinstance(data, list):
        raise ValueError("expected a JSON array of metric records")
    if not data:
        raise ValueError("input contains no metric records")
    if not isinstance(data[0], dict) or 'value' not in data[0]:
        if isinstance(data[0], dict) and any(
                key.endswith('Latency') for key in data[0]):
            raise ValueError(
                "this is a podLatencyMeasurement file; use pod-latency-stats instead"
            )
        raise ValueError("expected metric records with a 'value' field")
    if any(not isinstance(entry, dict) or 'value' not in entry for entry in data):
        raise ValueError("every metric record must contain a 'value' field")


class _Tee:
    """Write terminal output to both stdout and an open report file."""

    def __init__(self, *streams):
        self.streams = streams

    def write(self, text):
        for stream in self.streams:
            stream.write(text)
        return len(text)

    def flush(self):
        for stream in self.streams:
            stream.flush()


CSV_COLUMNS = [
    'label_key', 'label_value', 'n', 'mean', 'std', 'cv_percent',
    'min', 'p25', 'median', 'p75', 'p95', 'max',
]


def _write_label_stats_csv(path, rows):
    """Write all rows from the per-label-dimension display tables."""
    with path.open('w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _analyze_file(filepath, csv_path=None):
    data, display_name = _open_input(filepath)
    _validate_metric_records(data)
    print(f"\n{'#'*110}")
    print(f"# Analyzing: {display_name}")
    print(f"{'#'*110}")

    metric_type = detect_metric_type(data)
    unit = "bytes" if metric_type == 'memory' else "cores"
    print(f"Metric type: {metric_type} ({unit})")
    print(f"Total records: {len(data)}")

    w = fmt_val_header(metric_type)
    fv = lambda v: fmt_val(v, metric_type)

    # Auto-detect label keys from data
    label_keys_set = set()
    for entry in data[:200]:
        labels = entry.get('labels', {})
        label_keys_set.update(labels.keys())
    # Filter out labels that are constant across all records (uninformative)
    label_keys = []
    for lk in sorted(label_keys_set):
        unique = set()
        for entry in data:
            v = entry.get('labels', {}).get(lk)
            if v is not None:
                unique.add(v)
            if len(unique) > 1:
                break
        if len(unique) > 1:
            label_keys.append(lk)

    print(f"Label dimensions: {label_keys}")

    groups = {lk: defaultdict(list) for lk in label_keys}

    for entry in data:
        v = float(entry['value'])
        labels = entry.get('labels', {})
        for lk in label_keys:
            lv = labels.get(lk)
            if lv:
                groups[lk][lv].append(v)

    # Per-label-dimension tables
    csv_rows = []
    for lk in label_keys:
        grp = groups[lk]
        print(f"\n{'='*120}")
        print(f"LABEL: {lk}  ({len(grp)} unique values)")
        print(f"{'='*120}")

        all_stats = {}
        for lv, vals in sorted(grp.items()):
            st = compute_stats(vals)
            if st:
                all_stats[lv] = st

        sorted_by_mean = sorted(all_stats.items(), key=lambda x: x[1]['mean'], reverse=True)

        # Keep the complete table data in the CSV.  Terminal output remains
        # capped at 80 rows so large reports are still readable.
        for lv, st in sorted_by_mean:
            csv_rows.append({
                'label_key': lk, 'label_value': lv, 'n': st['n'],
                'mean': st['mean'], 'std': st['std'], 'cv_percent': st['cv'],
                'min': st['min'], 'p25': st['p25'], 'median': st['median'],
                'p75': st['p75'], 'p95': st['p95'], 'max': st['max'],
            })

        hdr = (f"{'Label Value':<65} {'N':>5} {'Mean':>{w}} {'Std':>{w}} {'CV%':>8} "
               f"{'Min':>{w}} {'P25':>{w}} {'Median':>{w}} {'P75':>{w}} {'P95':>{w}} {'Max':>{w}}")
        print(f"\n{hdr}")
        print("-" * len(hdr))
        for lv, st in sorted_by_mean[:80]:
            print(f"{lv:<65} {st['n']:>5} {fv(st['mean']):>{w}} {fv(st['std']):>{w}} {st['cv']:>8.1f} "
                  f"{fv(st['min']):>{w}} {fv(st['p25']):>{w}} {fv(st['median']):>{w}} "
                  f"{fv(st['p75']):>{w}} {fv(st['p95']):>{w}} {fv(st['max']):>{w}}")

    # Clustering: pick label dimensions with 3-150 unique values (informative groupings)
    mag_bands = get_magnitude_bands(metric_type)

    cluster_labels = [lk for lk in label_keys
                      if 3 <= len(groups[lk]) <= 150]
    if not cluster_labels:
        # Fallback: pick the two labels with most unique values
        cluster_labels = sorted(label_keys, key=lambda lk: len(groups[lk]), reverse=True)[:2]

    print(f"\n\n{'='*120}")
    print("STATISTICAL CLUSTERING - Grouping labels with similar behavior")
    print(f"{'='*120}")

    for lk in cluster_labels:
        grp = groups[lk]
        all_stats = {}
        for lv, vals in grp.items():
            st = compute_stats(vals)
            if st and st['n'] >= 3:
                all_stats[lv] = st

        print(f"\n--- {lk} clustering by magnitude ---")
        for lo, hi, label in mag_bands:
            members = [(lv, st) for lv, st in all_stats.items() if lo <= st['mean'] < hi]
            if members:
                members.sort(key=lambda x: x[1]['mean'], reverse=True)
                print(f"\n  {label}: ({len(members)} labels)")
                for lv, st in members:
                    print(f"    {lv:<55} mean={fv(st['mean'])}  std={fv(st['std'])}  "
                          f"CV={st['cv']:.1f}%  [P25={fv(st['p25'])}, P75={fv(st['p75'])}]")

        print(f"\n--- {lk} clustering by variability (CV) ---")
        for lo, hi, label in CV_BANDS:
            members = [(lv, st) for lv, st in all_stats.items() if lo <= st['cv'] < hi]
            if members:
                members.sort(key=lambda x: x[1]['cv'])
                print(f"\n  {label}: ({len(members)} labels)")
                for lv, st in members:
                    print(f"    {lv:<55} CV={st['cv']:.1f}%  mean={fv(st['mean'])}  "
                          f"std={fv(st['std'])}  [P5={fv(st['p5'])}, P95={fv(st['p95'])}]")

        print(f"\n--- {lk} combined clustering (magnitude x variability) ---")
        for mlo, mhi, mlabel in mag_bands:
            for clo, chi, clabel in CV_BANDS:
                members = [(lv, st) for lv, st in all_stats.items()
                           if mlo <= st['mean'] < mhi and clo <= st['cv'] < chi]
                if members:
                    members.sort(key=lambda x: x[1]['mean'], reverse=True)
                    print(f"\n  [{mlabel}] x [{clabel}]: ({len(members)} labels)")
                    for lv, st in members:
                        print(f"    {lv:<55} mean={fv(st['mean'])}  CV={st['cv']:.1f}%  "
                              f"median={fv(st['median'])}  IQR={fv(st['iqr'])}")

    # Anomaly detection: use the label with most unique values in cluster range,
    # or the first label with > 1 unique value
    anomaly_label = cluster_labels[0] if cluster_labels else (label_keys[0] if label_keys else None)
    if not anomaly_label:
        print("\nNo label dimensions found for anomaly detection.")
        if csv_path:
            _write_label_stats_csv(csv_path, csv_rows)
        return

    print(f"\n\n{'='*120}")
    print(f"ANOMALY DETECTION (by {anomaly_label})")
    print(f"{'='*120}")

    anomaly_stats = {}
    for lv, vals in groups[anomaly_label].items():
        st = compute_stats(vals)
        if st and st['n'] >= 3:
            anomaly_stats[lv] = st

    print(f"\n--- {anomaly_label} values with highest spike ratio (max/mean > 3x) ---")
    spikers = [(lv, st, st['max'] / st['mean']) for lv, st in anomaly_stats.items() if st['mean'] > 0]
    spikers.sort(key=lambda x: x[2], reverse=True)
    for lv, st, ratio in spikers:
        if ratio > 3:
            print(f"  {lv:<55} spike_ratio={ratio:.1f}x  mean={fv(st['mean'])}  "
                  f"max={fv(st['max'])}  P95={fv(st['p95'])}")

    metric_label = "CPU" if metric_type == 'cpu' else "Memory"
    print(f"\n--- Top 20 {anomaly_label} values by P95 {metric_label} ---")
    by_p95 = sorted(anomaly_stats.items(), key=lambda x: x[1]['p95'], reverse=True)[:20]
    for lv, st in by_p95:
        print(f"  {lv:<55} P95={fv(st['p95'])}  mean={fv(st['mean'])}  "
              f"max={fv(st['max'])}  CV={st['cv']:.1f}%")

    min_mean = get_min_mean_threshold(metric_type)
    print(f"\n--- Most predictable {anomaly_label} values (lowest CV, mean > {fv(min_mean)}) ---")
    predictable = [(lv, st) for lv, st in anomaly_stats.items() if st['mean'] > min_mean]
    predictable.sort(key=lambda x: x[1]['cv'])
    for lv, st in predictable[:20]:
        print(f"  {lv:<55} CV={st['cv']:.1f}%  mean={fv(st['mean'])}  IQR={fv(st['iqr'])}")

    if csv_path:
        _write_label_stats_csv(csv_path, csv_rows)


def analyze_file(filepath, output_dir=None, output_stem=None):
    """Analyze one input and optionally save its report and table data.

    ``output_stem`` is supplied by ``main`` to make colliding input basenames
    unambiguous when several files are exported into one directory.
    """
    if output_dir is None:
        return _analyze_file(filepath)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = output_stem or ('stdin' if filepath == '-' else Path(filepath).stem)
    report_path = output_dir / f"{stem}-report.txt"
    csv_path = output_dir / f"{stem}-label-stats.csv"

    original_stdout = sys.stdout
    with report_path.open('w', encoding='utf-8') as report_file:
        sys.stdout = _Tee(original_stdout, report_file)
        try:
            _analyze_file(filepath, csv_path)
        finally:
            sys.stdout = original_stdout

    print(f"Wrote report: {report_path}")
    print(f"Wrote CSV: {csv_path}")
    return report_path, csv_path


def main():
    parser = argparse.ArgumentParser(
        description="Statistical analysis of per-container time-series JSON files"
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "-o", "--output-dir", metavar="DIR",
        help="write a .txt report and label-statistics CSV for each input to DIR"
    )
    parser.add_argument(
        "files", nargs="+", metavar="file.json",
        help="one or more metric JSON files with {timestamp, labels, value} records"
    )
    args = parser.parse_args()

    used_stems = set()
    for filepath in args.files:
        base_stem = 'stdin' if filepath == '-' else Path(filepath).stem
        stem = base_stem
        suffix = 2
        while stem in used_stems:
            stem = f"{base_stem}-{suffix}"
            suffix += 1
        used_stems.add(stem)
        try:
            analyze_file(filepath, args.output_dir, stem)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            parser.error(f"{filepath}: {exc}")


if __name__ == '__main__':
    main()
