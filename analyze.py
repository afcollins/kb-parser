#!/usr/bin/env python3
"""
Comprehensive analysis of OpenShift cluster-density-v2 collected metrics.

Usage:
    python3 analyze.py                  # Analyze single run
    python3 analyze.py /path/to/other   # Compare this run against another run

Reads all JSON metric files from the script's directory (or a specified directory),
extracts values, and produces a detailed report including:
  - Per-category breakdowns (CPU, Memory, Etcd, API latency)
  - Multi-entry file distributions (per-container, per-resource breakdowns)
  - Avg vs Max comparisons with spike ratios
  - Etcd health assessment
  - Cluster utilization summary
  - Optional cross-run comparison with deltas and percent changes
"""
import json
import os
import sys
import statistics


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REQUESTED_FILES = [
    'max-memory-sum-masters.json', 'max-cpu-cluster-usage-ratio.json',
    'memory-cluster-usage-ratio.json', 'max-memory-sum-workers.json',
    'memory-ovnkube-node.json', 'max-memory-router.json',
    'max-memory-masters.json', 'memory-ovn-control-plane.json',
    'max-memory-etcd.json', 'memory-etcd.json', 'memory-sum-workers.json',
    'memory-sum-workers-start.json', 'max-memory-cluster-usage-ratio.json',
    'max-mutating-apicalls-latency.json', 'max-memory-crio.json',
    'max-memory-ovn-control-plane.json', 'max-cpu-workers.json',
    'max-memory-sum-kubelet.json', 'memory-prometheus.json',
    'max-memory-kube-apiserver.json', 'memory-kubelet.json',
    'memory-openshift-apiserver.json', 'max-memory-sum-kube-apiserver.json',
    'memory-workers.json', 'max-cpu-prometheus.json',
    'max-memory-sum-infra.json', 'memory-masters.json',
    'memory-kube-controller-manager.json',
    'max-memory-openshift-controller-manager.json',
    'max-memory-sum-etcd.json', 'memory-router.json', 'max-cpu-masters.json',
    'max-memory-multus.json', 'max-cpu-multus.json',
    'max-memory-sum-crio.json', 'max-memory-ovnkube-node.json',
    'max-memory-kubelet.json', 'max-memory-workers.json', 'memory-crio.json',
    'max-memory-sum-openshift-apiserver.json',
    'max-memory-kube-controller-manager.json', 'max-memory-prometheus.json',
    'memory-kube-apiserver.json', 'max-cpu-kubelet.json',
    'max-cpu-openshift-apiserver.json',
    'max-memory-sum-kube-controller-manager.json',
    'max-cpu-openshift-controller-manager.json',
    'max-ro-apicalls-latency.json', 'memory-infra.json',
    'max-memory-openshift-apiserver.json',
    'max-cpu-kube-controller-manager.json', 'memory-multus.json',
    'max-cpu-kube-apiserver.json', 'max-99thEtcdRoundTripTime.json',
    'max-cpu-router.json', 'max-cpu-ovn-control-plane.json',
    'max-cpu-infra.json', 'max-cpu-etcd.json', 'max-cpu-crio.json',
    'max-cpu-ovnkube-node.json', 'max-99thEtcdDiskWalFsync.json',
    'max-99thEtcdDefrag.json', 'max-99thEtcdCompaction.json',
    'max-99thEtcdDiskBackendCommit.json', 'cpu-prometheus.json',
    'cpu-ovnkube-node.json', 'cpu-workers.json', 'cpu-multus.json',
    'cpu-ovn-control-plane.json', 'cpu-openshift-controller-manager.json',
    'cpu-openshift-apiserver.json', 'cpu-kube-apiserver.json',
    'cpu-router.json', 'cpu-kubelet.json', 'cpu-masters.json',
    'cpu-crio.json', 'cpu-kube-controller-manager.json', 'cpu-infra.json',
    'cpu-etcd.json', 'cpu-cluster-usage-ratio.json',
    'memory-openshift-controller-manager.json', 'memory-prometheus.json',
    'memory-kubelet.json', 'memory-openshift-apiserver.json',
    'memory-workers.json', 'memory-masters.json',
    'memory-kube-controller-manager.json', 'memory-router.json',
    'memory-crio.json', 'memory-kube-apiserver.json', 'memory-infra.json',
    'memory-multus.json', 'memory-etcd.json', 'memory-ovnkube-node.json',
    'memory-ovn-control-plane.json', 'memory-sum-workers.json',
    'memory-sum-workers-start.json', 'memory-cluster-usage-ratio.json',
]

COMPONENTS = [
    'etcd', 'kube-apiserver', 'openshift-apiserver',
    'kube-controller-manager', 'openshift-controller-manager',
    'kubelet', 'crio', 'ovnkube-node', 'ovn-control-plane',
    'prometheus', 'router', 'multus',
]
NODE_LEVELS = ['masters', 'workers', 'infra']

# Etcd health thresholds (seconds)
ETCD_THRESHOLDS = {
    'Round Trip Time': 0.01,
    'Disk WAL Fsync': 0.01,
    'Disk Backend Commit': 0.025,
    'Compaction': 0.1,
    'Defrag': 1.0,
}

AVG_MAX_PAIRS = [
    ('cpu-etcd', 'max-cpu-etcd'),
    ('cpu-kube-apiserver', 'max-cpu-kube-apiserver'),
    ('cpu-openshift-apiserver', 'max-cpu-openshift-apiserver'),
    ('cpu-kube-controller-manager', 'max-cpu-kube-controller-manager'),
    ('cpu-openshift-controller-manager', 'max-cpu-openshift-controller-manager'),
    ('cpu-kubelet', 'max-cpu-kubelet'),
    ('cpu-crio', 'max-cpu-crio'),
    ('cpu-ovnkube-node', 'max-cpu-ovnkube-node'),
    ('cpu-ovn-control-plane', 'max-cpu-ovn-control-plane'),
    ('cpu-prometheus', 'max-cpu-prometheus'),
    ('cpu-router', 'max-cpu-router'),
    ('cpu-multus', 'max-cpu-multus'),
    ('memory-etcd', 'max-memory-etcd'),
    ('memory-kube-apiserver', 'max-memory-kube-apiserver'),
    ('memory-openshift-apiserver', 'max-memory-openshift-apiserver'),
    ('memory-kube-controller-manager', 'max-memory-kube-controller-manager'),
    ('memory-openshift-controller-manager', 'max-memory-openshift-controller-manager'),
    ('memory-kubelet', 'max-memory-kubelet'),
    ('memory-crio', 'max-memory-crio'),
    ('memory-ovnkube-node', 'max-memory-ovnkube-node'),
    ('memory-ovn-control-plane', 'max-memory-ovn-control-plane'),
    ('memory-prometheus', 'max-memory-prometheus'),
    ('memory-router', 'max-memory-router'),
    ('memory-multus', 'max-memory-multus'),
]


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
def fmt_bytes(b):
    if b is None:
        return 'N/A'
    if abs(b) >= 1e9:
        return f"{b / 1e9:.2f} GB"
    if abs(b) >= 1e6:
        return f"{b / 1e6:.2f} MB"
    if abs(b) >= 1e3:
        return f"{b / 1e3:.2f} KB"
    return f"{b:.2f} B"


def fmt_cpu(c):
    if c is None:
        return 'N/A'
    return f"{c:.4f} cores"


def fmt_pct(v):
    if v is None:
        return 'N/A'
    return f"{v * 100:.2f}%" if v <= 1 else f"{v:.2f}%"


def fmt_ms(v):
    if v is None:
        return 'N/A'
    if v < 1:
        return f"{v * 1000:.2f} ms"
    return f"{v:.4f} s"


def fmt_delta(old, new, is_bytes=False):
    """Format a comparison delta with percent change."""
    if old is None or new is None:
        return 'N/A', 'N/A'
    delta = new - old
    pct = (delta / old * 100) if old != 0 else float('inf')
    fmt = fmt_bytes if is_bytes else (lambda x: f"{x:.6f}")
    sign = '+' if delta >= 0 else ''
    return f"{sign}{fmt(delta)}", f"{sign}{pct:.1f}%"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_run(directory):
    """Load all requested JSON files from a directory. Returns dict of filename -> list of entries."""
    seen = set()
    unique = []
    for f in REQUESTED_FILES:
        if f not in seen:
            seen.add(f)
            unique.append(f)

    data = {}
    for f in unique:
        path = os.path.join(directory, f)
        if not os.path.exists(path):
            data[f] = None
            continue
        with open(path) as fh:
            raw = json.load(fh)
        entries = []
        for item in raw:
            entries.append({
                'value': item.get('value'),
                'metricName': item.get('metricName', ''),
                'query': item.get('query', ''),
                'timestamp': item.get('timestamp', ''),
                'metadata': item.get('metadata', {}),
            })
        data[f] = entries
    return data


def get_val(data, fname):
    """Get the first value from a file's entries."""
    d = data.get(fname)
    if d and len(d) > 0:
        return d[0]['value']
    return None


def get_all_vals(data, fname):
    """Get all values from a file's entries as a list."""
    d = data.get(fname)
    if not d:
        return []
    return [e['value'] for e in d if e['value'] is not None]


def get_metadata(data):
    """Extract OCP version and timestamp from first available file."""
    for entries in data.values():
        if entries and len(entries) > 0:
            meta = entries[0].get('metadata', {})
            return {
                'version': meta.get('ocpVersion', 'unknown'),
                'timestamp': entries[0].get('timestamp', 'unknown'),
            }
    return {'version': 'unknown', 'timestamp': 'unknown'}


# ---------------------------------------------------------------------------
# Categorization
# ---------------------------------------------------------------------------
def categorize(data):
    """Sort files into logical categories. Returns dict of category -> [(name, first_value)]."""
    categories = {
        'CPU - Per Component (avg cores)': [],
        'CPU - Max Per Component (cores)': [],
        'CPU - Node Level (avg cores)': [],
        'CPU - Max Node Level (cores)': [],
        'CPU - Cluster Usage Ratio': [],
        'Memory - Per Component (avg RSS)': [],
        'Memory - Max Per Component (RSS)': [],
        'Memory - Node Level (avg RSS)': [],
        'Memory - Max Node Level (RSS)': [],
        'Memory - Max Sum Aggregates': [],
        'Memory - Cluster Usage Ratio': [],
        'Memory - Worker Sum': [],
        'Etcd Performance (99th percentile)': [],
        'API Call Latency': [],
    }

    for f, entries in data.items():
        if entries is None:
            continue
        val = entries[0]['value'] if entries else None
        name = f.replace('.json', '')

        if name.startswith('cpu-cluster-usage-ratio'):
            categories['CPU - Cluster Usage Ratio'].append((name, val))
        elif name.startswith('max-cpu-cluster-usage-ratio'):
            categories['CPU - Cluster Usage Ratio'].append((name, val))
        elif name.startswith('memory-cluster-usage-ratio'):
            categories['Memory - Cluster Usage Ratio'].append((name, val))
        elif name.startswith('max-memory-cluster-usage-ratio'):
            categories['Memory - Cluster Usage Ratio'].append((name, val))
        elif name.startswith('max-99th'):
            categories['Etcd Performance (99th percentile)'].append((name, val))
        elif 'apicalls-latency' in name:
            categories['API Call Latency'].append((name, val))
        elif name.startswith('memory-sum-workers'):
            categories['Memory - Worker Sum'].append((name, val))
        elif name.startswith('max-memory-sum-'):
            categories['Memory - Max Sum Aggregates'].append((name, val))
        elif name.startswith('max-cpu-'):
            comp = name.replace('max-cpu-', '')
            if comp in NODE_LEVELS:
                categories['CPU - Max Node Level (cores)'].append((name, val))
            else:
                categories['CPU - Max Per Component (cores)'].append((name, val))
        elif name.startswith('cpu-'):
            comp = name.replace('cpu-', '')
            if comp in NODE_LEVELS:
                categories['CPU - Node Level (avg cores)'].append((name, val))
            else:
                categories['CPU - Per Component (avg cores)'].append((name, val))
        elif name.startswith('max-memory-'):
            comp = name.replace('max-memory-', '')
            if comp in NODE_LEVELS:
                categories['Memory - Max Node Level (RSS)'].append((name, val))
            else:
                categories['Memory - Max Per Component (RSS)'].append((name, val))
        elif name.startswith('memory-'):
            comp = name.replace('memory-', '')
            if comp in NODE_LEVELS:
                categories['Memory - Node Level (avg RSS)'].append((name, val))
            else:
                categories['Memory - Per Component (avg RSS)'].append((name, val))

    return categories


# ---------------------------------------------------------------------------
# Multi-entry distribution analysis
# ---------------------------------------------------------------------------
def analyze_multi_entry_files(data):
    """For files with multiple entries (per-container breakdowns), show distribution."""
    multi = {}
    for f, entries in data.items():
        if entries is not None and len(entries) > 1:
            vals = [e['value'] for e in entries if e['value'] is not None]
            if vals:
                multi[f] = {
                    'count': len(vals),
                    'min': min(vals),
                    'max': max(vals),
                    'mean': statistics.mean(vals),
                    'median': statistics.median(vals),
                    'stdev': statistics.stdev(vals) if len(vals) > 1 else 0,
                    'values': sorted(vals),
                }
    return multi


# ---------------------------------------------------------------------------
# Printing
# ---------------------------------------------------------------------------
def print_header(data):
    meta = get_metadata(data)
    print("=" * 110)
    print("OPENSHIFT CLUSTER-DENSITY-V2 PERFORMANCE ANALYSIS (120 NODES)")
    print(f"OCP Version: {meta['version']}")
    print(f"Timestamp:   {meta['timestamp']}")
    print("=" * 110)


def print_categories(categories):
    for cat, items in categories.items():
        if not items:
            continue
        print(f"\n{'─' * 110}")
        print(f"  {cat}")
        print(f"{'─' * 110}")
        items_sorted = sorted(items, key=lambda x: x[1] if x[1] is not None else 0, reverse=True)
        for name, val in items_sorted:
            if 'ratio' in cat.lower():
                print(f"  {name:<60} {fmt_pct(val)}")
            elif 'cpu' in cat.lower():
                print(f"  {name:<60} {fmt_cpu(val)}")
            elif 'memory' in cat.lower():
                print(f"  {name:<60} {fmt_bytes(val)}")
            elif 'etcd' in cat.lower():
                print(f"  {name:<60} {fmt_ms(val)}")
            elif 'latency' in cat.lower():
                print(f"  {name:<60} {fmt_ms(val)}")
            else:
                print(f"  {name:<60} {val}")


def print_multi_entry_distributions(multi):
    print(f"\n{'=' * 110}")
    print("MULTI-ENTRY FILE DISTRIBUTIONS (per-container / per-resource breakdowns)")
    print(f"{'=' * 110}")

    # Group by type
    cpu_multi = {k: v for k, v in multi.items()
                 if k.startswith('cpu-') or k.startswith('max-cpu-')}
    mem_multi = {k: v for k, v in multi.items()
                 if k.startswith('memory-') or k.startswith('max-memory-')}
    latency_multi = {k: v for k, v in multi.items()
                     if 'latency' in k or 'apicalls' in k}
    other_multi = {k: v for k, v in multi.items()
                   if k not in cpu_multi and k not in mem_multi and k not in latency_multi}

    for group_name, group in [('CPU', cpu_multi), ('Memory', mem_multi),
                               ('Latency', latency_multi), ('Other', other_multi)]:
        if not group:
            continue
        print(f"\n  --- {group_name} ---")
        for fname, stats in sorted(group.items()):
            name = fname.replace('.json', '')
            is_mem = group_name == 'Memory'
            is_lat = group_name == 'Latency'
            fmt = fmt_bytes if is_mem else (fmt_ms if is_lat else fmt_cpu)
            print(f"\n  {name} ({stats['count']} entries)")
            print(f"    Min:    {fmt(stats['min'])}")
            print(f"    Max:    {fmt(stats['max'])}")
            print(f"    Mean:   {fmt(stats['mean'])}")
            print(f"    Median: {fmt(stats['median'])}")
            if stats['stdev'] > 0:
                print(f"    Stdev:  {fmt(stats['stdev'])}")
            # Show all values sorted for small sets, or a histogram summary for large ones
            if stats['count'] <= 10:
                vals_str = ', '.join(fmt(v) for v in stats['values'])
                print(f"    All values (sorted): [{vals_str}]")
            else:
                # Show percentile distribution
                vals = stats['values']
                p10 = vals[int(len(vals) * 0.10)]
                p25 = vals[int(len(vals) * 0.25)]
                p50 = vals[int(len(vals) * 0.50)]
                p75 = vals[int(len(vals) * 0.75)]
                p90 = vals[int(len(vals) * 0.90)]
                p95 = vals[int(len(vals) * 0.95)]
                p99 = vals[int(len(vals) * 0.99)]
                print(f"    P10:    {fmt(p10)}")
                print(f"    P25:    {fmt(p25)}")
                print(f"    P50:    {fmt(p50)}")
                print(f"    P75:    {fmt(p75)}")
                print(f"    P90:    {fmt(p90)}")
                print(f"    P95:    {fmt(p95)}")
                print(f"    P99:    {fmt(p99)}")
            # Skew analysis
            if stats['mean'] > 0 and stats['count'] > 2:
                skew_ratio = stats['max'] / stats['mean']
                if skew_ratio > 5:
                    print(f"    ** HIGHLY SKEWED: max is {skew_ratio:.1f}x the mean (outlier containers dominate)")
                elif skew_ratio > 2:
                    print(f"    ** MODERATELY SKEWED: max is {skew_ratio:.1f}x the mean")


def print_avg_vs_max(data):
    print(f"\n{'=' * 110}")
    print("COMPARATIVE ANALYSIS: AVG vs MAX (spike detection)")
    print(f"{'=' * 110}")
    print(f"\n  {'Component':<40} {'Avg':<20} {'Max':<20} {'Spike Ratio':<15} {'Assessment'}")
    print(f"  {'─' * 105}")

    for avg_f, max_f in AVG_MAX_PAIRS:
        avg_data = data.get(avg_f + '.json')
        max_data = data.get(max_f + '.json')
        if not avg_data or not max_data:
            continue
        avg_val = avg_data[0]['value']
        max_val = max_data[0]['value']
        ratio = max_val / avg_val if avg_val and avg_val != 0 else 0

        is_mem = 'memory' in avg_f
        comp = avg_f.replace('cpu-', '').replace('memory-', '')
        metric_type = 'MEM' if is_mem else 'CPU'

        # Assess spike severity
        if ratio > 5:
            assessment = '*** MAJOR SPIKES ***'
        elif ratio > 3:
            assessment = '** Notable spikes'
        elif ratio > 2:
            assessment = '* Moderate spikes'
        else:
            assessment = 'Stable'

        if is_mem:
            print(f"  {metric_type + ' ' + comp:<40} {fmt_bytes(avg_val):<20} {fmt_bytes(max_val):<20} {ratio:.2f}x          {assessment}")
        else:
            print(f"  {metric_type + ' ' + comp:<40} {fmt_cpu(avg_val):<20} {fmt_cpu(max_val):<20} {ratio:.2f}x          {assessment}")


def print_key_findings(data):
    print(f"\n{'=' * 110}")
    print("KEY FINDINGS SUMMARY")
    print(f"{'=' * 110}")

    # Cluster utilization
    cpu_ratio = get_val(data, 'cpu-cluster-usage-ratio.json')
    max_cpu_ratio = get_val(data, 'max-cpu-cluster-usage-ratio.json')
    mem_ratio = get_val(data, 'memory-cluster-usage-ratio.json')
    max_mem_ratio = get_val(data, 'max-memory-cluster-usage-ratio.json')

    print(f"\n  CLUSTER UTILIZATION:")
    print(f"    CPU Usage Ratio:    avg={fmt_pct(cpu_ratio)}, max={fmt_pct(max_cpu_ratio)}")
    print(f"    Memory Usage Ratio: avg={fmt_pct(mem_ratio)}, max={fmt_pct(max_mem_ratio)}")
    if max_cpu_ratio and max_cpu_ratio > 0.7:
        print(f"    ** CPU is heavily loaded (max > 70%)")
    if max_mem_ratio and max_mem_ratio > 0.7:
        print(f"    ** Memory is heavily loaded (max > 70%)")

    # Top CPU consumers
    print(f"\n  TOP CPU CONSUMERS (max values):")
    cpu_max = []
    for f_name, entries in data.items():
        if entries and f_name.startswith('max-cpu-') and not any(x in f_name for x in ['cluster', 'masters', 'workers', 'infra']):
            cpu_max.append((f_name.replace('.json', '').replace('max-cpu-', ''), entries[0]['value']))
    cpu_max.sort(key=lambda x: x[1], reverse=True)
    for comp, val in cpu_max[:5]:
        print(f"    {comp:<40} {fmt_cpu(val)}")

    # Top Memory consumers
    print(f"\n  TOP MEMORY CONSUMERS (max values):")
    mem_max = []
    for f_name, entries in data.items():
        if entries and f_name.startswith('max-memory-') and not any(x in f_name for x in ['sum', 'cluster', 'masters', 'workers', 'infra']):
            mem_max.append((f_name.replace('.json', '').replace('max-memory-', ''), entries[0]['value']))
    mem_max.sort(key=lambda x: x[1], reverse=True)
    for comp, val in mem_max[:5]:
        print(f"    {comp:<40} {fmt_bytes(val)}")

    # Etcd health
    print(f"\n  ETCD HEALTH:")
    etcd_metrics = {
        'Round Trip Time': get_val(data, 'max-99thEtcdRoundTripTime.json'),
        'Disk WAL Fsync': get_val(data, 'max-99thEtcdDiskWalFsync.json'),
        'Disk Backend Commit': get_val(data, 'max-99thEtcdDiskBackendCommit.json'),
        'Compaction': get_val(data, 'max-99thEtcdCompaction.json'),
        'Defrag': get_val(data, 'max-99thEtcdDefrag.json'),
    }
    for metric, val in etcd_metrics.items():
        threshold = ETCD_THRESHOLDS.get(metric)
        status = ''
        if val is not None and threshold is not None:
            status = ' *** WARNING ***' if val > threshold else ' [OK]'
        print(f"    {metric:<35} {fmt_ms(val)}{status}")

    # Worker memory growth
    print(f"\n  WORKER MEMORY AGGREGATE:")
    mem_sum_w = get_val(data, 'memory-sum-workers.json')
    mem_sum_w_start = get_val(data, 'memory-sum-workers-start.json')
    max_mem_sum_w = get_val(data, 'max-memory-sum-workers.json')
    if mem_sum_w_start and mem_sum_w:
        growth = mem_sum_w - mem_sum_w_start
        growth_pct = (growth / mem_sum_w_start) * 100 if mem_sum_w_start else 0
        print(f"    Start:   {fmt_bytes(mem_sum_w_start)}")
        print(f"    End:     {fmt_bytes(mem_sum_w)}")
        print(f"    Max:     {fmt_bytes(max_mem_sum_w)}")
        print(f"    Growth:  {fmt_bytes(growth)} ({growth_pct:+.1f}%)")
        if growth_pct > 20:
            print(f"    ** SIGNIFICANT MEMORY GROWTH during test")
        elif growth_pct < -10:
            print(f"    ** Memory DECREASED during test (GC / pod churn)")

    # API latency
    print(f"\n  API CALL LATENCY (max p99):")
    ro_lat = get_val(data, 'max-ro-apicalls-latency.json')
    mut_lat = get_val(data, 'max-mutating-apicalls-latency.json')
    print(f"    Read-only:  {fmt_ms(ro_lat)}")
    print(f"    Mutating:   {fmt_ms(mut_lat)}")
    if ro_lat and ro_lat > 1.0:
        print(f"    ** Read-only latency exceeds 1s SLO!")
    if mut_lat and mut_lat > 1.0:
        print(f"    ** Mutating latency exceeds 1s SLO!")

    # API latency distribution (multi-entry)
    ro_vals = get_all_vals(data, 'max-ro-apicalls-latency.json')
    mut_vals = get_all_vals(data, 'max-mutating-apicalls-latency.json')
    if len(ro_vals) > 1:
        print(f"\n  API LATENCY DISTRIBUTION (across resource types):")
        print(f"    Read-only ({len(ro_vals)} resources):")
        print(f"      Min: {fmt_ms(min(ro_vals))}, Max: {fmt_ms(max(ro_vals))}, "
              f"Mean: {fmt_ms(statistics.mean(ro_vals))}, Median: {fmt_ms(statistics.median(ro_vals))}")
        over_200ms = sum(1 for v in ro_vals if v > 0.2)
        over_500ms = sum(1 for v in ro_vals if v > 0.5)
        over_1s = sum(1 for v in ro_vals if v > 1.0)
        print(f"      >200ms: {over_200ms}, >500ms: {over_500ms}, >1s: {over_1s}")
    if len(mut_vals) > 1:
        print(f"    Mutating ({len(mut_vals)} resources):")
        print(f"      Min: {fmt_ms(min(mut_vals))}, Max: {fmt_ms(max(mut_vals))}, "
              f"Mean: {fmt_ms(statistics.mean(mut_vals))}, Median: {fmt_ms(statistics.median(mut_vals))}")
        over_200ms = sum(1 for v in mut_vals if v > 0.2)
        over_500ms = sum(1 for v in mut_vals if v > 0.5)
        over_1s = sum(1 for v in mut_vals if v > 1.0)
        print(f"      >200ms: {over_200ms}, >500ms: {over_500ms}, >1s: {over_1s}")

    # Memory sum aggregates
    print(f"\n  MEMORY SUM AGGREGATES (max across all nodes of each type):")
    sum_items = []
    for f_name, entries in data.items():
        if entries and f_name.startswith('max-memory-sum-'):
            sum_items.append((f_name.replace('.json', '').replace('max-memory-sum-', ''), entries[0]['value']))
    sum_items.sort(key=lambda x: x[1], reverse=True)
    for comp, val in sum_items:
        print(f"    {comp:<40} {fmt_bytes(val)}")


def print_cpu_distribution_analysis(data):
    """Analyze the distribution pattern of all cpu-* values."""
    print(f"\n{'=' * 110}")
    print("CPU VALUE DISTRIBUTION ANALYSIS")
    print(f"{'=' * 110}")

    # Collect all avg cpu component values
    avg_vals = {}
    max_vals = {}
    for f, entries in data.items():
        if entries is None:
            continue
        name = f.replace('.json', '')
        if name.startswith('cpu-') and 'cluster' not in name and name.replace('cpu-', '') not in NODE_LEVELS:
            # For multi-entry files, sum all container values to get total component CPU
            total = sum(e['value'] for e in entries if e['value'] is not None)
            avg_vals[name.replace('cpu-', '')] = total
        elif name.startswith('max-cpu-') and 'cluster' not in name and name.replace('max-cpu-', '') not in NODE_LEVELS:
            total = sum(e['value'] for e in entries if e['value'] is not None)
            max_vals[name.replace('max-cpu-', '')] = total

    if not avg_vals:
        return

    all_avg = sorted(avg_vals.values(), reverse=True)
    total_avg = sum(all_avg)

    print(f"\n  Component CPU share (avg, total = {fmt_cpu(total_avg)}):")
    print(f"  {'Component':<35} {'Avg CPU':<18} {'% of Total':<12} {'Cumulative %':<12} {'Bar'}")
    print(f"  {'─' * 100}")
    cumulative = 0
    for comp, val in sorted(avg_vals.items(), key=lambda x: x[1], reverse=True):
        pct = (val / total_avg * 100) if total_avg > 0 else 0
        cumulative += pct
        bar = '#' * int(pct / 2)
        print(f"  {comp:<35} {fmt_cpu(val):<18} {pct:>6.1f}%      {cumulative:>6.1f}%      {bar}")

    print(f"\n  Distribution shape:")
    if len(all_avg) >= 2:
        top2_pct = sum(all_avg[:2]) / total_avg * 100
        print(f"    Top 2 components account for {top2_pct:.1f}% of total CPU")
    if len(all_avg) >= 3:
        top3_pct = sum(all_avg[:3]) / total_avg * 100
        print(f"    Top 3 components account for {top3_pct:.1f}% of total CPU")
    gini = compute_gini(all_avg)
    print(f"    Gini coefficient: {gini:.3f} (0=equal, 1=all in one component)")
    if gini > 0.7:
        print(f"    ** HIGHLY CONCENTRATED: CPU usage dominated by a few components")
    elif gini > 0.5:
        print(f"    ** MODERATELY CONCENTRATED")
    else:
        print(f"    ** RELATIVELY EVEN distribution")


def print_memory_distribution_analysis(data):
    """Analyze the distribution pattern of all memory-* values."""
    print(f"\n{'=' * 110}")
    print("MEMORY VALUE DISTRIBUTION ANALYSIS")
    print(f"{'=' * 110}")

    avg_vals = {}
    for f, entries in data.items():
        if entries is None:
            continue
        name = f.replace('.json', '')
        if name.startswith('memory-') and 'cluster' not in name and 'sum' not in name \
                and name.replace('memory-', '') not in NODE_LEVELS:
            total = sum(e['value'] for e in entries if e['value'] is not None)
            avg_vals[name.replace('memory-', '')] = total

    if not avg_vals:
        return

    all_avg = sorted(avg_vals.values(), reverse=True)
    total_avg = sum(all_avg)

    print(f"\n  Component Memory share (avg RSS, total = {fmt_bytes(total_avg)}):")
    print(f"  {'Component':<35} {'Avg RSS':<18} {'% of Total':<12} {'Cumulative %':<12} {'Bar'}")
    print(f"  {'─' * 100}")
    cumulative = 0
    for comp, val in sorted(avg_vals.items(), key=lambda x: x[1], reverse=True):
        pct = (val / total_avg * 100) if total_avg > 0 else 0
        cumulative += pct
        bar = '#' * int(pct / 2)
        print(f"  {comp:<35} {fmt_bytes(val):<18} {pct:>6.1f}%      {cumulative:>6.1f}%      {bar}")

    print(f"\n  Distribution shape:")
    if len(all_avg) >= 2:
        top2_pct = sum(all_avg[:2]) / total_avg * 100
        print(f"    Top 2 components account for {top2_pct:.1f}% of total Memory")
    gini = compute_gini(all_avg)
    print(f"    Gini coefficient: {gini:.3f} (0=equal, 1=all in one component)")


def compute_gini(values):
    """Compute Gini coefficient for a list of non-negative values."""
    if not values or sum(values) == 0:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    cumsum = 0
    total = sum(sorted_vals)
    for i, v in enumerate(sorted_vals):
        cumsum += (2 * (i + 1) - n - 1) * v
    return cumsum / (n * total)


# ---------------------------------------------------------------------------
# Cross-run comparison
# ---------------------------------------------------------------------------
def compare_runs(data_a, data_b, label_a="Run A", label_b="Run B"):
    """Compare two runs side by side."""
    print(f"\n{'=' * 110}")
    print(f"CROSS-RUN COMPARISON: {label_a} vs {label_b}")
    print(f"{'=' * 110}")

    meta_a = get_metadata(data_a)
    meta_b = get_metadata(data_b)
    print(f"\n  {label_a}: {meta_a['version']} @ {meta_a['timestamp']}")
    print(f"  {label_b}: {meta_b['version']} @ {meta_b['timestamp']}")

    # Compare cluster ratios
    print(f"\n  --- Cluster Utilization ---")
    for metric, fname in [('CPU ratio (avg)', 'cpu-cluster-usage-ratio.json'),
                           ('CPU ratio (max)', 'max-cpu-cluster-usage-ratio.json'),
                           ('Mem ratio (avg)', 'memory-cluster-usage-ratio.json'),
                           ('Mem ratio (max)', 'max-memory-cluster-usage-ratio.json')]:
        va = get_val(data_a, fname)
        vb = get_val(data_b, fname)
        d_str, p_str = fmt_delta(va, vb)
        print(f"    {metric:<30} {fmt_pct(va):<15} {fmt_pct(vb):<15} delta: {p_str}")

    # Compare component CPU
    print(f"\n  --- Component CPU (avg) ---")
    print(f"    {'Component':<35} {label_a:<18} {label_b:<18} {'Delta %':<12} {'Flag'}")
    print(f"    {'─' * 95}")
    for comp in COMPONENTS:
        fname = f'cpu-{comp}.json'
        va = get_val(data_a, fname)
        vb = get_val(data_b, fname)
        if va is None and vb is None:
            continue
        _, p_str = fmt_delta(va, vb)
        flag = ''
        if va and vb:
            pct = (vb - va) / va * 100 if va != 0 else 0
            if pct > 20:
                flag = '** REGRESSION'
            elif pct < -20:
                flag = '++ IMPROVEMENT'
        print(f"    {comp:<35} {fmt_cpu(va):<18} {fmt_cpu(vb):<18} {p_str:<12} {flag}")

    # Compare component Memory
    print(f"\n  --- Component Memory (avg RSS) ---")
    print(f"    {'Component':<35} {label_a:<18} {label_b:<18} {'Delta %':<12} {'Flag'}")
    print(f"    {'─' * 95}")
    for comp in COMPONENTS:
        fname = f'memory-{comp}.json'
        va = get_val(data_a, fname)
        vb = get_val(data_b, fname)
        if va is None and vb is None:
            continue
        _, p_str = fmt_delta(va, vb, is_bytes=True)
        flag = ''
        if va and vb:
            pct = (vb - va) / va * 100 if va != 0 else 0
            if pct > 20:
                flag = '** REGRESSION'
            elif pct < -20:
                flag = '++ IMPROVEMENT'
        print(f"    {comp:<35} {fmt_bytes(va):<18} {fmt_bytes(vb):<18} {p_str:<12} {flag}")

    # Compare Etcd health
    print(f"\n  --- Etcd Performance ---")
    etcd_files = {
        'Round Trip Time': 'max-99thEtcdRoundTripTime.json',
        'Disk WAL Fsync': 'max-99thEtcdDiskWalFsync.json',
        'Disk Backend Commit': 'max-99thEtcdDiskBackendCommit.json',
        'Compaction': 'max-99thEtcdCompaction.json',
        'Defrag': 'max-99thEtcdDefrag.json',
    }
    for metric, fname in etcd_files.items():
        va = get_val(data_a, fname)
        vb = get_val(data_b, fname)
        _, p_str = fmt_delta(va, vb)
        print(f"    {metric:<35} {fmt_ms(va):<18} {fmt_ms(vb):<18} {p_str}")

    # Compare API latency
    print(f"\n  --- API Latency ---")
    for metric, fname in [('Read-only (max p99)', 'max-ro-apicalls-latency.json'),
                           ('Mutating (max p99)', 'max-mutating-apicalls-latency.json')]:
        va = get_val(data_a, fname)
        vb = get_val(data_b, fname)
        _, p_str = fmt_delta(va, vb)
        print(f"    {metric:<35} {fmt_ms(va):<18} {fmt_ms(vb):<18} {p_str}")

    # Summary score
    print(f"\n  --- Regression Summary ---")
    regressions = 0
    improvements = 0
    for comp in COMPONENTS:
        for prefix in ['cpu-', 'memory-']:
            fname = f'{prefix}{comp}.json'
            va = get_val(data_a, fname)
            vb = get_val(data_b, fname)
            if va and vb and va != 0:
                pct = (vb - va) / va * 100
                if pct > 20:
                    regressions += 1
                elif pct < -20:
                    improvements += 1
    print(f"    Components with >20% regression:  {regressions}")
    print(f"    Components with >20% improvement: {improvements}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data = load_run(base_dir)

    # Single-run analysis
    print_header(data)
    categories = categorize(data)
    print_categories(categories)
    multi = analyze_multi_entry_files(data)
    print_multi_entry_distributions(multi)
    print_avg_vs_max(data)
    print_cpu_distribution_analysis(data)
    print_memory_distribution_analysis(data)
    print_key_findings(data)

    # Cross-run comparison if a second directory is provided
    if len(sys.argv) > 1:
        other_dir = sys.argv[1]
        if not os.path.isdir(other_dir):
            print(f"\nERROR: {other_dir} is not a directory", file=sys.stderr)
            sys.exit(1)
        data_b = load_run(other_dir)
        meta_a = get_metadata(data)
        meta_b = get_metadata(data_b)
        compare_runs(data, data_b,
                      label_a=f"This Run ({meta_a['version'][:30]})",
                      label_b=f"Other Run ({meta_b['version'][:30]})")

    print(f"\n{'=' * 110}")
    print("END OF ANALYSIS")
    print(f"{'=' * 110}")


if __name__ == '__main__':
    main()
