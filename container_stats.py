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
import contextlib
import csv
import io
import json
from pathlib import Path
from importlib.metadata import version, PackageNotFoundError
import sys
import statistics
from collections import defaultdict

try:
    __version__ = version("kb-parser")
except PackageNotFoundError:
    # package is not installed
    pass


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


LABEL_STATS_COLUMNS = [
    'label_key', 'label_value', 'n', 'mean', 'std', 'cv_percent',
    'min', 'p25', 'median', 'p75', 'p95', 'max',
]

# Identifies a CSV row whose statistics cover every record for one UUID/job,
# rather than a single label dimension/value.
ALL_LABELS_KEY = '__all_labels__'
ALL_LABELS_VALUE = '__all__'
ALL_JOBS_VALUE = '__all_jobs__'

# Add or remove entries here to control the metadata columns emitted in every
# label-statistics CSV row.  Each path is tried in order, which accommodates
# jobSummary.json files from kube-burner versions that used either
# ``churnConfig`` or ``workloadFlags``.
JOB_SUMMARY_CSV_FIELDS = [
    ('cluster_name', (('clusterName',),)),
    ('ocp_version', (('ocpVersion',),)),
    ('kube_burner_version', (('version',),)),
    ('worker_nodes', (('workerNodesCount',),)),
    ('total_nodes', (('totalNodes',),)),
    ('workload', (('jobConfig', 'name'),)),
    ('iterations', (('jobConfig', 'jobIterations'),)),
    ('start_timestamp', (('timestamp',),)),
    ('end_timestamp', (('endTimestamp',),)),
    ('uuid', (('uuid',),)),
    ('churn_cycles', (
        ('jobConfig', 'churnConfig', 'cycles'),
        ('jobConfig', 'workloadFlags', 'churnCycles'),
        ('jobConfig', 'churnCycles'),
        ('workloadFlags', 'churnCycles'),
    )),
    ('churn_percent', (
        ('jobConfig', 'churnConfig', 'percent'),
        ('jobConfig', 'workloadFlags', 'churnPercent'),
        ('jobConfig', 'churnPercent'),
        ('workloadFlags', 'churnPercent'),
    )),
    ('churn_duration', (
        ('jobConfig', 'churnConfig', 'duration'),
        ('jobConfig', 'workloadFlags', 'churnDuration'),
        ('jobConfig', 'churnDuration'),
        ('workloadFlags', 'churnDuration'),
    )),
    ('churn_delay', (
        ('jobConfig', 'churnConfig', 'delay'),
        ('jobConfig', 'workloadFlags', 'churnDelay'),
        ('jobConfig', 'churnDelay'),
        ('workloadFlags', 'churnDelay'),
    )),
    ('churn_deletion_strategy', (
        ('jobConfig', 'churnConfig', 'deletionStrategy'),
        ('jobConfig', 'workloadFlags', 'churnDeletionStrategy'),
        ('jobConfig', 'churnDeletionStrategy'),
        ('workloadFlags', 'deletionStrategy'),
    )),
]

CSV_METADATA_COLUMNS = [name for name, _ in JOB_SUMMARY_CSV_FIELDS] + [
    'metric_name', 'job_name',
]
CSV_COLUMNS = CSV_METADATA_COLUMNS + LABEL_STATS_COLUMNS


def _get_nested_value(document, path):
    """Return a nested value, or None when a path is not present."""
    value = document
    for key in path:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


def _load_job_summaries(filepath):
    """Load optional job-summary documents beside a metric file."""
    if filepath == '-':
        return []
    summary_path = Path(filepath).parent / 'jobSummary.json'
    if not summary_path.exists():
        return []
    with summary_path.open(encoding='utf-8') as summary_file:
        summaries = json.load(summary_file)
    if isinstance(summaries, dict):
        return [summaries]
    if not isinstance(summaries, list) or any(not isinstance(item, dict) for item in summaries):
        raise ValueError('jobSummary.json must contain an object or an array of objects')
    return summaries


def _job_summary_csv_values(summary):
    """Flatten configured job-summary paths into CSV-safe scalar values."""
    values = {}
    for name, paths in JOB_SUMMARY_CSV_FIELDS:
        values[name] = ''
        for path in paths:
            value = _get_nested_value(summary, path)
            if value is not None:
                values[name] = value
                break
    return values


def _matching_job_summary(summaries, uuid, job_name):
    """Return the job-summary document for one metric UUID/job partition."""
    matching_uuid = [summary for summary in summaries
                     if not uuid or summary.get('uuid') == uuid]
    for summary in matching_uuid:
        if summary.get('jobConfig', {}).get('name') == job_name:
            return summary
    return matching_uuid[0] if len(matching_uuid) == 1 else {}


def _uuid_workload_summary(summaries, uuid):
    """Select the primary workload summary for a UUID-wide aggregate.

    RDS is the primary workload for multi-job RDS runs.  Otherwise, a
    two-workload run containing the cleanup job uses its non-cleanup workload.
    Ambiguous combinations intentionally have no selected workload.
    """
    matching_uuid = [summary for summary in summaries
                     if not uuid or summary.get('uuid') == uuid]
    by_name = {
        summary.get('jobConfig', {}).get('name'): summary
        for summary in matching_uuid
        if summary.get('jobConfig', {}).get('name')
    }
    if 'rds' in by_name:
        return by_name['rds']
    if len(by_name) == 1:
        return next(iter(by_name.values()))
    if len(by_name) == 2 and 'garbage-collection' in by_name:
        non_cleanup = [summary for name, summary in by_name.items()
                       if name != 'garbage-collection']
        return non_cleanup[0] if len(non_cleanup) == 1 else {}
    return {}


def _shared_job_summary_csv_values(summaries, uuid):
    """Return UUID-wide metadata derived from every matching job summary."""
    matching_uuid = [summary for summary in summaries
                     if not uuid or summary.get('uuid') == uuid]
    if not matching_uuid:
        return _job_summary_csv_values({})

    values_by_summary = [_job_summary_csv_values(summary)
                         for summary in matching_uuid]
    shared = {}
    for field in CSV_METADATA_COLUMNS:
        values = {str(summary_values.get(field, ''))
                  for summary_values in values_by_summary}
        shared[field] = values.pop() if len(values) == 1 else ''

    # ISO 8601 UTC timestamps sort chronologically as strings.  Unlike the
    # other job-specific fields, the UUID-wide row represents the full run,
    # so retain its earliest start and latest end.
    start_times = [summary.get('timestamp') for summary in matching_uuid
                   if summary.get('timestamp')]
    end_times = [summary.get('endTimestamp') for summary in matching_uuid
                 if summary.get('endTimestamp')]
    shared['start_timestamp'] = min(start_times) if start_times else ''
    shared['end_timestamp'] = max(end_times) if end_times else ''

    iteration_values = []
    for summary in matching_uuid:
        value = _get_nested_value(summary, ('jobConfig', 'jobIterations'))
        if value is None or isinstance(value, bool):
            continue
        try:
            iteration_values.append(float(value))
        except (TypeError, ValueError):
            continue
    if iteration_values:
        max_iterations = max(iteration_values)
        shared['iterations'] = (
            int(max_iterations) if max_iterations.is_integer() else max_iterations
        )

    workload_summary = _uuid_workload_summary(summaries, uuid)
    if workload_summary:
        shared['workload'] = workload_summary.get('jobConfig', {}).get('name', '')
    return shared


def _metric_partitions(data):
    """Group metric records by UUID, metricName, and jobName before aggregating."""
    partitions = defaultdict(list)
    for entry in data:
        key = (
            str(entry.get('uuid', '')),
            str(entry.get('metricName', '')),
            str(entry.get('jobName', '')),
        )
        partitions[key].append(entry)
    return partitions


def _append_csv_stats_row(rows, metadata, label_key, label_value, stats):
    """Append one summary-statistics row using the shared CSV schema."""
    rows.append({
        **metadata,
        'label_key': label_key,
        'label_value': label_value,
        'n': stats['n'],
        'mean': stats['mean'],
        'std': stats['std'],
        'cv_percent': stats['cv'],
        'min': stats['min'],
        'p25': stats['p25'],
        'median': stats['median'],
        'p75': stats['p75'],
        'p95': stats['p95'],
        'max': stats['max'],
    })


def _write_label_stats_csv(path, rows):
    """Write all rows from the per-label-dimension display tables."""
    with path.open('w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _analyze_data(data, display_name, csv_rows, metadata):
    print(f"\n{'#'*110}")
    print(f"# Analyzing: {display_name}")
    print(f"{'#'*110}")

    metric_type = detect_metric_type(data)
    unit = "bytes" if metric_type == 'memory' else "cores"
    print(f"Metric type: {metric_type} ({unit})")
    print(f"Total records: {len(data)}")

    w = fmt_val_header(metric_type)
    fv = lambda v: fmt_val(v, metric_type)

    # A single all-labels row makes cross-run comparisons possible without
    # needing to re-aggregate the per-label rows after concatenating CSVs.
    overall_stats = compute_stats([float(entry['value']) for entry in data])
    _append_csv_stats_row(
        csv_rows, metadata, ALL_LABELS_KEY, ALL_LABELS_VALUE, overall_stats
    )

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
            _append_csv_stats_row(csv_rows, metadata, lk, lv, st)

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



def _analyze_records(filepath, data, display_name, csv_path=None):
    job_summaries = _load_job_summaries(filepath)
    csv_rows = []
    partitions = _metric_partitions(data)
    uuid_partitions = defaultdict(list)
    uuid_label_values = defaultdict(list)

    for (record_uuid, metric_name, job_name), job_data in sorted(partitions.items()):
        summary = _matching_job_summary(job_summaries, record_uuid, job_name)
        summary_metadata = _job_summary_csv_values(summary)
        effective_uuid = record_uuid or summary_metadata['uuid']
        workload_summary = _uuid_workload_summary(job_summaries, effective_uuid)
        workload = workload_summary.get('jobConfig', {}).get('name', '')
        uuid_partitions[(effective_uuid, metric_name)].extend(job_data)
        for entry in job_data:
            for label_key, label_value in entry.get('labels', {}).items():
                uuid_label_values[(effective_uuid, metric_name, label_key, label_value)].append(
                    float(entry['value'])
                )
        metadata = {
            **summary_metadata,
            'uuid': effective_uuid,
            'workload': workload,
            'metric_name': metric_name,
            'job_name': job_name,
        }
        job_display_name = display_name
        if len(partitions) > 1:
            job_display_name = (
                f"{display_name} [uuid={metadata['uuid'] or '<missing>'}, "
                f"metricName={metric_name or '<missing>'}, "
                f"jobName={job_name or '<missing>'}]"
            )
        _analyze_data(job_data, job_display_name, csv_rows, metadata)

    # Add per-label UUID-wide statistics, combining each label value across
    # every job in the run.
    for (uuid, metric_name, label_key, label_value), values in sorted(uuid_label_values.items()):
        metadata = _shared_job_summary_csv_values(job_summaries, uuid)
        _append_csv_stats_row(
            csv_rows,
            {
                **metadata,
                'uuid': uuid,
                'metric_name': metric_name,
                'job_name': ALL_JOBS_VALUE,
            },
            label_key,
            label_value,
            compute_stats(values),
        )

    # Add a UUID-wide aggregate after the individual job partitions. This lets
    # consumers compare a whole run without re-aggregating its job rows.
    for (uuid, metric_name), uuid_data in sorted(uuid_partitions.items()):
        stats = compute_stats([float(entry['value']) for entry in uuid_data])
        metadata = _shared_job_summary_csv_values(job_summaries, uuid)
        _append_csv_stats_row(
            csv_rows,
            {
                **metadata,
                'uuid': uuid,
                'metric_name': metric_name,
                'job_name': ALL_JOBS_VALUE,
            },
            ALL_LABELS_KEY,
            ALL_LABELS_VALUE,
            stats,
        )

    if csv_path:
        _write_label_stats_csv(csv_path, csv_rows)


def _analyze_file(filepath, csv_path=None):
    data, display_name = _open_input(filepath)
    _validate_metric_records(data)
    _analyze_records(filepath, data, display_name, csv_path)


def analyze_file(filepath, output_dir=None, output_stem=None, quiet=False):
    """Analyze one input and optionally save its report and table data.

    ``output_stem`` is supplied by ``main`` to make colliding input basenames
    unambiguous when several files are exported into one directory.
    """
    if output_dir is None:
        if quiet:
            with contextlib.redirect_stdout(io.StringIO()):
                return _analyze_file(filepath)
        return _analyze_file(filepath)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = output_stem or ('stdin' if filepath == '-' else Path(filepath).stem)
    report_path = output_dir / f"{stem}-report.txt"
    csv_path = output_dir / f"{stem}-label-stats.csv"

    if quiet:
        with report_path.open('w', encoding='utf-8') as report_file:
            with contextlib.redirect_stdout(report_file):
                _analyze_file(filepath, csv_path)
        return report_path, csv_path

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
        "-q", "--quiet", action="store_true",
        help="suppress terminal output; with --output-dir, save the report and CSV files"
    )
    parser.add_argument(
        "files", nargs="+", metavar="file.json",
        help="one or more metric JSON files with {timestamp, labels, value} records"
    )
    args = parser.parse_args()

    used_stems = set()
    for filepath in args.files:
        path = Path(filepath)
        base_stem = 'stdin' if filepath == '-' else path.stem
        stem = base_stem
        suffix = 2
        while stem in used_stems:
            stem = f"{base_stem}-{suffix}"
            suffix += 1
        used_stems.add(stem)
        try:
            analyze_file(filepath, args.output_dir, stem, args.quiet)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            parser.error(f"{filepath}: {exc}")


if __name__ == '__main__':
    main()
