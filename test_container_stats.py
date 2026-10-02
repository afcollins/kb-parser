"""Regression tests for container-stats CSV aggregation metadata."""

import contextlib
import csv
import io
import json
import statistics
import sys
from pathlib import Path
from unittest.mock import patch

import container_stats as CS
import metric_delta as MD


TESTDATA_DIR = Path(__file__).parent / 'testdata'
MULTI_JOB_DIR = TESTDATA_DIR / 'collected-metrics-b334187f-b4e4-4e80-ab20-63a937fa7aea'
MULTI_JOB_METRIC = MULTI_JOB_DIR / 'kubeletCPU.json'
MULTI_JOB_UUID = 'b334187f-b4e4-4e80-ab20-63a937fa7aea'
CGROUP_START = MULTI_JOB_DIR / 'cgroupCPUSeconds-Workers-start.json'
CGROUP_FINAL = MULTI_JOB_DIR / 'cgroupCPUSeconds-Workers.json'


class TestUuidWideAggregation:
    def test_start_and_final_files_create_deduplicated_deltas(self, tmp_path):
        start_data = json.loads(CGROUP_START.read_text())
        final_data = json.loads(CGROUP_FINAL.read_text())

        deltas = MD.build_delta_records(start_data, final_data)

        assert len(deltas) == 15  # 3 jobs x 5 cgroup labels; replicas are averaged.
        rds_pods = next(
            entry for entry in deltas
            if entry['jobName'] == 'rds'
            and entry['labels']['id'] == '/kubepods.slice'
        )
        key = MD._snapshot_key(rds_pods)
        expected = (
            statistics.mean(float(entry['value']) for entry in final_data
                            if MD._snapshot_key(entry) == key)
            - statistics.mean(float(entry['value']) for entry in start_data
                              if MD._snapshot_key(entry) == key)
        )
        assert rds_pods['value'] == expected
        assert rds_pods['metricName'] == 'cgroupCPUSeconds-Workers-delta'

        csv_path = MD.write_delta_csv(CGROUP_START, tmp_path)
        assert csv_path.name == 'cgroupCPUSeconds-Workers.csv'
        with csv_path.open(newline='') as csv_file:
            rows = list(csv.DictReader(csv_file))
        assert {row['metric_name'] for row in rows} == {
            'cgroupCPUSeconds-Workers-delta'
        }
        assert 'mean' not in rows[0]
        assert 'delta' in rows[0]
        raw_rds = [row for row in rows if row['job_name'] == 'rds'
                   and row['label_key'] != CS.ALL_LABELS_KEY]
        rds_total = next(row for row in rows if row['job_name'] == 'rds'
                         and row['label_key'] == CS.ALL_LABELS_KEY
                         and row['label_value'] == CS.ALL_LABELS_VALUE)
        uuid_total = next(row for row in rows
                          if row['job_name'] == CS.ALL_JOBS_VALUE
                          and row['label_key'] == CS.ALL_LABELS_KEY)
        uuid_pods = next(row for row in rows
                         if row['job_name'] == CS.ALL_JOBS_VALUE
                         and row['label_key'] == 'id'
                         and row['label_value'] == '/kubepods.slice')
        assert len(raw_rds) == 5
        assert float(rds_total['delta']) == sum(float(row['delta']) for row in raw_rds)
        assert uuid_total['label_key'] == CS.ALL_LABELS_KEY
        assert uuid_total['label_value'] == CS.ALL_LABELS_VALUE
        assert float(uuid_pods['delta']) == sum(
            float(row['delta']) for row in rows
            if row['job_name'] != CS.ALL_JOBS_VALUE
            and row['label_key'] == 'id'
            and row['label_value'] == '/kubepods.slice'
        )
        assert float(uuid_total['delta']) == sum(
            float(row['delta']) for row in rows
            if row['job_name'] != CS.ALL_JOBS_VALUE
            and row['label_key'] != CS.ALL_LABELS_KEY
        )

    def test_start_file_cli_finds_final_file(self, tmp_path, capsys):
        with patch.object(sys, 'argv', [
            'metric-delta', '--quiet', '--output-dir', str(tmp_path),
            str(CGROUP_START),
        ]):
            MD.main()

        assert (tmp_path / 'cgroupCPUSeconds-Workers.csv').is_file()
        assert capsys.readouterr().out == ''

    def test_container_stats_still_analyzes_start_file_normally(self, tmp_path, capsys):
        with patch.object(sys, 'argv', [
            'container-stats', '--quiet', '--output-dir', str(tmp_path),
            str(CGROUP_START),
        ]):
            CS.main()

        assert (tmp_path / 'cgroupCPUSeconds-Workers-start-label-stats.csv').is_file()
        assert not (tmp_path / 'cgroupCPUSeconds-Workers.csv').exists()
        assert capsys.readouterr().out == ''

    def test_metric_name_partitions_are_not_merged(self):
        records = [
            {'uuid': 'run-1', 'metricName': 'containerCPU', 'jobName': 'rds'},
            {'uuid': 'run-1', 'metricName': 'containerMemory', 'jobName': 'rds'},
        ]

        partitions = CS._metric_partitions(records)

        assert set(partitions) == {
            ('run-1', 'containerCPU', 'rds'),
            ('run-1', 'containerMemory', 'rds'),
        }

    def test_job_summary_metadata_columns_are_mapped(self):
        summary = {
            'clusterName': 'test-cluster',
            'ocpVersion': '4.99.0-test',
            'version': '1.2.3@test',
            'workerNodesCount': 12,
            'totalNodes': 15,
            'timestamp': '2026-01-01T00:00:00Z',
            'endTimestamp': '2026-01-01T00:05:00Z',
            'uuid': 'test-uuid',
            'jobConfig': {
                'name': 'node-density',
                'jobIterations': 42,
                'churnConfig': {
                    'cycles': 3,
                    'percent': 25,
                    'duration': 60,
                    'delay': 5,
                    'deletionStrategy': 'foreground',
                },
            },
        }

        values = CS._job_summary_csv_values(summary)

        assert values == {
            'cluster_name': 'test-cluster',
            'ocp_version': '4.99.0-test',
            'kube_burner_version': '1.2.3@test',
            'worker_nodes': 12,
            'total_nodes': 15,
            'workload': 'node-density',
            'iterations': 42,
            'start_timestamp': '2026-01-01T00:00:00Z',
            'end_timestamp': '2026-01-01T00:05:00Z',
            'uuid': 'test-uuid',
            'churn_cycles': 3,
            'churn_percent': 25,
            'churn_duration': 60,
            'churn_delay': 5,
            'churn_deletion_strategy': 'foreground',
        }

    def test_top_level_workload_flags_are_used_as_churn_fallbacks(self):
        summary = {
            'workloadFlags': {
                'churnCycles': '4',
                'churnPercent': '30',
                'churnDuration': '2m',
                'churnDelay': '15s',
                'deletionStrategy': 'default',
            },
        }

        values = CS._job_summary_csv_values(summary)

        assert values['churn_cycles'] == '4'
        assert values['churn_percent'] == '30'
        assert values['churn_duration'] == '2m'
        assert values['churn_delay'] == '15s'
        assert values['churn_deletion_strategy'] == 'default'

    def test_quiet_output_dir_writes_report_and_csv_without_stdout(self, tmp_path, capsys):
        CS.analyze_file(MULTI_JOB_METRIC, tmp_path, quiet=True)

        assert (tmp_path / 'kubeletCPU-label-stats.csv').is_file()
        report_path = tmp_path / 'kubeletCPU-report.txt'
        assert report_path.is_file()
        assert report_path.read_text(encoding='utf-8')
        assert capsys.readouterr().out == ''

    def test_rds_is_the_uuid_workload_for_every_row(self, tmp_path):
        """RDS wins over setup and garbage-collection jobs for one UUID."""
        csv_path = tmp_path / 'kubeletCPU-label-stats.csv'
        with contextlib.redirect_stdout(io.StringIO()):
            CS._analyze_file(MULTI_JOB_METRIC, csv_path)

        with csv_path.open(newline='') as csv_file:
            rows = list(csv.DictReader(csv_file))

        assert rows
        assert {row['uuid'] for row in rows} == {MULTI_JOB_UUID}
        assert {row['metric_name'] for row in rows} == {'kubeletCPU'}
        assert {row['workload'] for row in rows} == {'rds'}
        assert {
            row['job_name'] for row in rows
        } >= {'bgp-setup', 'rds', 'garbage-collection', CS.ALL_JOBS_VALUE}

        overall_rows = {
            row['job_name']: row
            for row in rows
            if row['label_key'] == CS.ALL_LABELS_KEY
        }
        assert overall_rows['rds']['n'] == '2'
        assert overall_rows['bgp-setup']['n'] == '2'
        assert overall_rows['garbage-collection']['n'] == '2'
        assert overall_rows[CS.ALL_JOBS_VALUE]['n'] == '6'
        assert overall_rows[CS.ALL_JOBS_VALUE]['workload'] == 'rds'
        assert overall_rows[CS.ALL_JOBS_VALUE]['iterations'] == '9'

        all_jobs_node = next(
            row for row in rows
            if row['job_name'] == CS.ALL_JOBS_VALUE
            and row['label_key'] == 'node'
            and row['label_value'] == 'cc37-h27-000-r750'
        )
        assert all_jobs_node['n'] == '3'

    def test_non_cleanup_workload_is_selected_for_two_job_run(self):
        summaries = [
            {'uuid': 'run-1', 'jobConfig': {'name': 'node-density'}},
            {'uuid': 'run-1', 'jobConfig': {'name': 'garbage-collection'}},
        ]

        selected = CS._uuid_workload_summary(summaries, 'run-1')

        assert selected['jobConfig']['name'] == 'node-density'
