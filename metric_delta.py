#!/usr/bin/env python3
"""Calculate final-minus-start deltas for kube-burner metric snapshots."""
import argparse
import csv
from collections import defaultdict
from pathlib import Path
import statistics

import container_stats as stats


CSV_COLUMNS = stats.CSV_METADATA_COLUMNS + ['label_key', 'label_value', 'delta']


def _snapshot_key(entry):
    return (
        str(entry.get('uuid', '')),
        str(entry.get('jobName', '')),
        tuple(sorted(entry.get('labels', {}).items())),
    )


def build_delta_records(start_data, final_data):
    """Average duplicate snapshots, then calculate final-minus-start values."""
    start_groups = defaultdict(list)
    final_groups = defaultdict(list)
    for entry in start_data:
        start_groups[_snapshot_key(entry)].append(entry)
    for entry in final_data:
        final_groups[_snapshot_key(entry)].append(entry)

    records = []
    for key in sorted(start_groups.keys() & final_groups.keys()):
        starts = start_groups[key]
        finals = final_groups[key]
        # Records may be from different Prometheus replicas, but source identity
        # is absent. Their mean is a practical estimate, not a measurement with
        # a defensible propagated standard error.
        start_value = statistics.mean(float(entry['value']) for entry in starts)
        final_value = statistics.mean(float(entry['value']) for entry in finals)
        final = finals[0]
        metric_name = final.get('metricName', '')
        delta = final_value - start_value
        records.append({
            **final,
            'metricName': f'{metric_name}-delta' if metric_name else 'delta',
            'value': delta,
            'delta': delta,
        })
    if not records:
        raise ValueError('no records matched between start and final metric files')
    return records


def _final_path(start_path):
    path = Path(start_path)
    if not path.stem.endswith('-start'):
        raise ValueError('input filename must end in -start.json')
    return path.with_name(f'{path.stem[:-6]}{path.suffix}')


def write_delta_csv(start_filepath, output_dir=None):
    final_filepath = _final_path(start_filepath)
    if not final_filepath.is_file():
        raise ValueError(f'corresponding final metric file not found: {final_filepath}')

    start_data, _ = stats._open_input(start_filepath)
    final_data, _ = stats._open_input(final_filepath)
    stats._validate_metric_records(start_data)
    stats._validate_metric_records(final_data)
    records = build_delta_records(start_data, final_data)
    summaries = stats._load_job_summaries(final_filepath)

    rows = []
    job_totals = defaultdict(float)
    label_totals = defaultdict(float)
    uuid_totals = defaultdict(float)
    for record in records:
        uuid = str(record.get('uuid', ''))
        job_name = str(record.get('jobName', ''))
        summary = stats._matching_job_summary(summaries, uuid, job_name)
        metadata = stats._job_summary_csv_values(summary)
        workload = stats._uuid_workload_summary(summaries, uuid)
        base_row = {
            **metadata,
            'uuid': uuid or metadata['uuid'],
            'metric_name': record['metricName'],
            'job_name': job_name,
            'workload': workload.get('jobConfig', {}).get('name', ''),
            'delta': record['delta'],
        }
        for label_key, label_value in sorted(record.get('labels', {}).items()):
            rows.append({
                **base_row,
                'label_key': label_key,
                'label_value': label_value,
            })
            label_totals[(base_row['uuid'], base_row['metric_name'],
                          label_key, label_value)] += record['delta']
        job_totals[(base_row['uuid'], base_row['metric_name'], job_name)] += record['delta']
        uuid_totals[(base_row['uuid'], base_row['metric_name'])] += record['delta']

    for (uuid, metric_name, job_name), delta in sorted(job_totals.items()):
        summary = stats._matching_job_summary(summaries, uuid, job_name)
        metadata = stats._job_summary_csv_values(summary)
        workload = stats._uuid_workload_summary(summaries, uuid)
        rows.append({
            **metadata,
            'uuid': uuid or metadata['uuid'],
            'metric_name': metric_name,
            'job_name': job_name,
            'workload': workload.get('jobConfig', {}).get('name', ''),
            'label_key': stats.ALL_LABELS_KEY,
            'label_value': stats.ALL_LABELS_VALUE,
            'delta': delta,
        })

    for (uuid, metric_name, label_key, label_value), delta in sorted(label_totals.items()):
        metadata = stats._shared_job_summary_csv_values(summaries, uuid)
        rows.append({
            **metadata,
            'uuid': uuid,
            'metric_name': metric_name,
            'job_name': stats.ALL_JOBS_VALUE,
            'label_key': label_key,
            'label_value': label_value,
            'delta': delta,
        })

    for (uuid, metric_name), delta in sorted(uuid_totals.items()):
        metadata = stats._shared_job_summary_csv_values(summaries, uuid)
        rows.append({
            **metadata,
            'uuid': uuid,
            'metric_name': metric_name,
            'job_name': stats.ALL_JOBS_VALUE,
            'label_key': stats.ALL_LABELS_KEY,
            'label_value': stats.ALL_LABELS_VALUE,
            'delta': delta,
        })

    output_dir = Path(output_dir) if output_dir else final_filepath.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f'{final_filepath.stem}.csv'
    with csv_path.open('w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return csv_path


def main():
    parser = argparse.ArgumentParser(
        description='Calculate final-minus-start deltas for metric snapshot files'
    )
    parser.add_argument('-o', '--output-dir', metavar='DIR', help='write CSV to DIR')
    parser.add_argument('-q', '--quiet', action='store_true', help='suppress completion output')
    parser.add_argument('files', nargs='+', metavar='file-start.json',
                        help='one or more *-start.json metric files')
    args = parser.parse_args()

    for filepath in args.files:
        try:
            csv_path = write_delta_csv(filepath, args.output_dir)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            parser.error(f'{filepath}: {exc}')
        if not args.quiet:
            print(f'Wrote CSV: {csv_path}')


if __name__ == '__main__':
    main()
