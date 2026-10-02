# kb-parser

CLI tools for parsing and visualizing performance metrics from **kube-burner-ocp** test runs.

# Install

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
make install
# or: uv tool install .
```

This installs three commands: `kb-parser`, `container-stats`, `pod-latency-stats`.

**Which one do I use?**

`kb-parser` can process multiple runs' worth metrics indexed locally and generate graphs. Plaintext graphs via plotille, or plotly (experimental).

`container-stats` and `pod-latency-stats` analyze an entire metrics file. Use `pod-latency-stats` for the `podLatencyMeasurement` file and `container-stats` for everything else. All labels used in the metrics are identified and used to group metrics for analysis. Summary statistics (e.g. avg, max, 99th-percentile) are calculated for each group, as well as a breakdown of how concentrated each group is based on standard deviation and spread.

# Usage

## kb-parser

Analyze latency and generic metric files with stats, histograms, and CDF plots.

```bash
# Direct file mode — pass any metric JSON file
kb-parser containerCPU.json
kb-parser podLatencyMeasurement-workload.json
```

### Some common workflows:
```bash
kb-parser containerCPU.json -g container --top-labels 150  # Repeat for all containers collected
kb-parser nodeCPU-Workers.json -g mode                     # See the spread of each cpu mode across all workers
kb-parser nodeCPU-Workers.json -g instance -l mode=idle -s # See the idle cpu for each instance
kb-parser cgroupCPU.json -g id                             # Spread of each cgroup ID (e.g. '/system.slice')

# UUID fragment mode — discovers files from kube-burner output dirs
kb-parser 2178a534
kb-parser 2178a534 metrics containerCPU
```

### Complete help
```bash
$ kb-parser -h
usage: kb-parser [-h] [--version] [--no-visuals] [--metric-name METRIC_NAME] [--label KEY=VALUE] [--min VALUE] [--max VALUE]
                 [--scatter] [--source] [--top-labels N] [--bucket "MIN, MAX"] [--tbucket "START_SEC, END_SEC"]
                 [--latency-type TYPE] [--agg] [--plotly] [--group-by KEY] [--no-query-cache] [--no-hist] [--no-cdf] [--verbose]
                 [--quiet]
                 [positionals ...]

Parse kube-burner metrics and produce CSV reports.

positional arguments:
  positionals           Direct metric file path(s), or UUID fragment(s) with optional 'metrics' keyword and metric file name(s).
                        Examples: 'containerCPU.json', '2178a534 metrics containerCPU'.

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --no-visuals          Disable the large terminal plots (scatterplot, histogram, CDF) to save space.
  --metric-name, -m METRIC_NAME
                        Display name for metric (metrics mode only; e.g. cgroupCPU).
  --label, -l KEY=VALUE
                        Filter by label in metrics mode (repeatable, e.g. -l id=/kubepods.slice).
  --min, -n VALUE       Only plot values >= VALUE (stats use full dataset).
  --max, -x VALUE       Only plot values <= VALUE (stats use full dataset).
  --scatter, -s         In metrics mode: render a scatter plot of value vs. elapsed time. Served from cache after the first run —
                        fast for iterative drilldown.
  --source, -S          In metrics mode: show label distributions for entries within the active value range
                        (--min/--max/--bucket). Always reads from source file on first run.
  --top-labels N        Max label values to show per key in cardinality output (default: 10).
  --bucket, -b "MIN, MAX"
                        Plot range from histogram bucket label, e.g. --bucket "12083200, 12096000". Overrides --min/--max if both
                        are given.
  --tbucket, -t "START_SEC, END_SEC"
                        Filter metrics to a time window by elapsed seconds from the scatter X-axis, e.g. --tbucket "30, 90".
                        Analogous to --bucket for values.
  --latency-type, -L TYPE
                        Latency field to analyze from podLatencyMeasurement JSON (unambiguous prefix accepted, e.g. -L s for
                        schedulingLatency). Choices: podReadyLatency, schedulingLatency, initializedLatency,
                        containersReadyLatency, readyToStartContainersLatency. Default: podReadyLatency.
  --agg                 Combine all fragments' data into one aggregated scatter/histogram/CDF instead of separate plots per
                        fragment.
  --plotly              Use plotly interactive HTML output (opens in browser) instead of plotille terminal plots. Compatible with
                        --agg.
  --group-by, -g KEY    Split metric entries by label key and show per-group statistics. Use --top-labels N to control how many
                        groups are shown (default 10).
  --query-cache         Enable per-query cache files (e.g. for -l / -b / -t). By default only the base cache is read/written.
  --no-hist             Suppress the frequency histogram plot.
  --no-cdf              Suppress the CDF plot.
  --verbose, -v         Show all timing detail including intermediate steps.
  --quiet, -q           Suppress all diagnostic output; show only stats tables and graphs.

kb-parser UUID — latency analysis. kb-parser UUID metrics FILE ... — metrics analysis (order of args is irrelevant). Examples:
'kb-parser UUID' or 'kb-parser UUID metrics containerCPU cgroupCPU'
```

## container-stats

Statistical analysis of per-container time-series JSON files.

```bash
container-stats containerCPU.json
container-stats containerMemory.json cgroupMemoryRSS.json
container-stats --output-dir stats-output containerCPU.json
container-stats --quiet --output-dir stats-output containerCPU.json

$ container-stats -h
usage: container-stats [-h] [--version] [-o DIR] file.json [file.json ...]

Statistical analysis of per-container time-series JSON files

positional arguments:
  file.json   one or more metric JSON files with {timestamp, labels, value} records

options:
  -h, --help  show this help message and exit
  --version   show program's version number and exit
  -o DIR, --output-dir DIR
                write a .txt report and label-statistics CSV for each input to DIR
  -q, --quiet   suppress terminal output; with --output-dir, save the report and CSV files
```

When `--output-dir` is provided, each input produces a `*-report.txt` copy of
the terminal report and a `*-label-stats.csv` file. The CSV combines the
per-label tables using `label_key` and `label_value` columns.
When the input is inside a collected-metrics directory, the CSV also repeats
run metadata from the neighboring `jobSummary.json` on every row, including
cluster/version/node, workload, timestamps, UUID, and churn settings. The
metric record's `metricName` and `jobName` are included as `metric_name` and
`job_name`, so outputs can be separated by metric, workload, and UUID. When
`jobSummary.json` has multiple documents, each job partition uses the document
whose `jobConfig.name` matches its `job_name` for job-specific fields. The
`workload` column is instead the UUID-wide primary workload, and therefore has
one value on every row for that UUID. The
metadata schema is configured by `JOB_SUMMARY_CSV_FIELDS` in
`container_stats.py`.

Use `--quiet --output-dir DIR` to write the normal report and CSV files without
printing the report or completion messages to the terminal.

## metric-delta

Calculate one final-minus-start delta per UUID, job, and label set. Supply the
`*-start.json` file; its matching final file is found in the same directory.

```bash
metric-delta --output-dir delta-output cgroupCPUSeconds-Workers-start.json
```

The resulting CSV has the usual run metadata plus `label_key`, `label_value`,
and `delta` columns; it does not include distribution statistics such as
percentiles or CV. A `label_key=__all_labels__`, `label_value=__all__` row
sums all label deltas for a job. A `job_name=__all_jobs__` row with
the original label key/value sums that label across every job; its
`label_key=__all_labels__`, `label_value=__all__` row sums every job for the
UUID.

Each UUID/job combination also has one all-records summary row, marked with
`label_key=__all_labels__` and `label_value=__all__`. Its statistics cover the
entire metric partition regardless of labels.
A second UUID-wide row combines every job for that run; it is marked with both
`job_name=__all_jobs__` and `label_key=__all_labels__`; its `label_value` is
`__all__`. Its `workload` is selected as `rds` when present; otherwise, a
two-job run containing `garbage-collection` uses the other workload. Its
`iterations` value is the maximum job iteration count. Other job-specific
metadata is left blank when the summaries differ.
Additional `job_name=__all_jobs__` rows preserve each original label key/value
and calculate its statistics across every job for that UUID.

## pod-latency-stats

Percentile band analysis of podLatencyMeasurement files.

```bash
pod-latency-stats podLatencyMeasurement-workload.json
pod-latency-stats --csv-only podLatencyMeasurement-workload.json
pod-latency-stats --output-dir latency-output podLatencyMeasurement-workload.json

$ pod-latency-stats -h
usage: pod-latency-stats [-h] [--version] [--csv-only] [-o DIR] file.json [file.json ...]

Analyze podLatencyMeasurement JSON files and output report + CSV

positional arguments:
  file.json   one or more podLatencyMeasurement JSON files

options:
  -h, --help  show this help message and exit
  --version   show program's version number and exit
  --csv-only  CSV output only, no report
  -o DIR, --output-dir DIR
                write reports and CSV exports to DIR
```

With `--output-dir`, each input produces a `*-report.txt`, a
`*-latency-summary.csv` (one summary-statistic row per latency type), and a
`*-latency-percentiles.csv` (P0–P100 rows by latency type). The existing
cross-run detailed CSV, `podLatency-percentile-bands.csv`, is written there too.

## Test

```bash
make test
```
