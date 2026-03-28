# Collected Metrics Analysis - Project Status

## Data Source

OpenShift 4.23.0-0.nightly-2026-03-23-202110, cluster-density-v2 test, 120-node AWS cluster.
UUID: `8fa05000-6d95-474d-8a2d-280e64ab0098`

Data directory:
```
~/Downloads/periodic-ci-openshift-eng-ocp-qe-perfscale-ci-main-aws-4.23-nightly-x86-control-plane-120nodes/
  2036261976056795136/artifacts/control-plane-120nodes/openshift-qe-cluster-density-v2/
  artifacts/collected-metrics-8fa05000-6d95-474d-8a2d-280e64ab0098/
```

## Scripts Written

| Script | Purpose | Input format |
|--------|---------|--------------|
| `analyze.py` | Summary metrics analysis (max-*, cpu-*, memory-* files) | Single-value or small-array JSON |
| `container_stats.py` | Per-label statistical analysis with clustering | Any `{timestamp, labels, value}` JSON |
| `pod_latency_stats.py` | Bimodal detection + percentile band CSV output | podLatencyMeasurement JSON |

## Completed Analysis

### 1. Summary metrics (max-*, cpu-*, memory-*)
- **Script**: `analyze.py`
- **Report**: Inline in conversation (first analysis)
- Key findings: prometheus + kube-apiserver dominate CPU/memory; etcd round trip borderline at 12.67ms; no memory leaks

### 2. containerCPU.json (39,629 records)
- **Script**: `container_stats.py`
- **Report**: `containerCPU-analysis-report.md`
- Labels: container (150), namespace (54), node (126), pod (1990)
- Key findings: 6 containers use >95% CPU; leader-election bimodality in kube-controller-manager; workers homogeneous

### 3. containerMemory.json (45,291 records)
- **Script**: `container_stats.py` (same script, auto-detects memory, uses scientific notation)
- Labels: container (154), namespace (55), node (126), pod (1990+)
- Verified working; no separate report written yet

### 4. podLatencyMeasurement-cluster-density-v2.json (14,040 records)
- **Script**: `pod_latency_stats.py`
- **Report**: `podLatencyMeasurement-analysis-report.md`
- **CSV**: `podLatency-percentile-bands.csv` (written alongside input file)
- Key findings: containersReadyLatency/podReadyLatency are bimodal (69% fast band 0-5s, 31% slow band 11-15s, 6s gap, separation quality 8.57). P68/P69 are the critical pair for detecting band-ratio shifts.

### 5. cgroupCPU.json / cgroupMemoryRSS.json / nodeCPU-* / nodeMemoryUtilization-*
- **Script**: `container_stats.py` (generalized to auto-detect labels)
- Verified working on all four types; no reports written yet
- cgroupCPU labels: id (5 cgroup slices), node (126)
- nodeCPU labels: instance (120 workers / 3 masters), mode (7 CPU modes)

### 6. Network, CRI-O, and Kubelet metrics (7 files)
- **Script**: `container_stats.py`
- All analyzed successfully; key findings:

| File | Records | Labels | Key Finding |
|------|---------|--------|-------------|
| `nodeTxNetwork.json` | 2,520 | device (2), instance (126) | 3 outlier nodes with 10-100x higher TX (likely control plane); all others homogeneous, high CV (bursty) |
| `nodeRxNetwork.json` | 2,520 | device (2), instance (126) | Same 3 outlier nodes + 2 more with elevated RX |
| `nodeMajorFaults.json` | 1,260 | instance (126), pod (126) | All near-zero — no memory pressure |
| `crioCPU.json` | 1,200 | node (120) | Mean 1.05–1.89 cores, homogeneous fleet |
| `crioMemory.json` | 1,200 | node (120) | Mean 151–180 MB, very stable (CV <11%) |
| `kubeletCPU.json` | 1,200 | node (120) | Mean 8.1–10.8 cores, 3 nodes >10 cores; kubelet ~7-8x heavier than CRI-O |
| `kubeletMemory.json` | 1,200 | node (120) | Mean 265–400 MB, CV 6–26%, stable |

## TODO - Remaining Files

### Label-grouped time-series (`container_stats.py` should work as-is)

| File | Records | Labels | Notes |
|------|---------|--------|-------|
| `cgroupCPUSeconds-*.json` | ? | ? | Cumulative CPU seconds (Workers/Masters/Infra/namespaces, plus -start variants) |
| `cgroupMemoryRSS-*.json` | ? | ? | RSS memory by role (Workers/Masters/Infra/namespaces, plus -start variants) |
| `nodeCPUSeconds-*.json` | ? | ? | Cumulative CPU seconds per node |
| `nodeCPU-Infra.json` | ? | instance, mode | Infra node CPU |
| `nodeMemoryUtilization-Infra.json` | ? | instance + k8s labels | Infra node memory |

### API latency and request rate — analyzed, see section 7 below.

### 7. API Latency and Request Rate (5 files)
- **Script**: `container_stats.py`
- SLO thresholds: mutating P99 < 1s, read-only P99 < 500ms

| File | Records | Labels | Key Finding |
|------|---------|--------|-------------|
| `mutatingAPICallsLatency.json` | 1,135 | resource (105), scope (2), verb (4) | **events P99 = 9.15s — SLO VIOLATION (9x over 1s)**; services max 1.76s; secrets/pods/configmaps borderline at P95 ~0.97s; ~98 other resources all 0.09–0.19s |
| `readOnlyAPICallsLatency.json` | 1,443 | resource (151), scope (3), verb (2) | All within 500ms SLO; namespace LIST mean 133ms max 197ms; one `images` GET outlier spike to 848ms |
| `avg-mutating-apicalls-latency.json` | 40 | resource (38), scope (2), verb (4) | Single-point averages; pods avg 595ms elevated; most resources 100–130ms |
| `avg-ro-apicalls-latency.json` | 58 | resource (49), scope (2), verb (2) | All under 155ms, no concerns |
| `APIRequestRate.json` | 2,728 | code (6), resource (157), verb (8) | subjectaccessreviews 30 req/s; pods/events/endpointslices extremely bursty (max 148–183 req/s); HTTP 403 bursts to 148 req/s |

- **SLO violations**: Only `events` clearly violates (mutating P99 9.15s). `services` breaches at max only. Read-only all clear.
- **Correlation**: High-latency resources (events, pods, endpointslices) are also the burstiest by request rate.

### Etcd metrics (mostly single-value or very few records)

| File | Records | Notes |
|------|---------|-------|
| `99thEtcdRoundTripTime.json` | 1 | Single value: 12.67ms (borderline, threshold 10ms) |
| `99thEtcdDiskBackendCommit.json` | ? | Single value |
| `99thEtcdDiskWalFsync.json` | ? | Single value |
| `99thEtcdCompaction.json` | 4 | Per-etcd-pod; value 3725 (likely histogram bucket) |
| `99thEtcdDefrag.json` | ? | Per-etcd-pod |
| `99thEtcdCompaction-raw.json` | 3 | Raw values: ~55599 (seconds? needs interpretation) |
| `99thEtcdCompaction-raw-start.json` | ? | Baseline snapshot |
| `99thEtcdDefrag-raw-start.json` | ? | Baseline snapshot |
| `99thEtcd*DurationSeconds.json` | ? | Alternate unit variants |
| `max-99thEtcd*.json` | ? | Already covered by `analyze.py` |

These are small enough to inspect directly. The `-raw` vs non-raw and `DurationSeconds` variants may need reconciliation to understand which represents actual latency.

### Service latency (different format - no `value` field)

| File | Records | Notes |
|------|---------|-------|
| `svcLatencyMeasurement-cluster-density-v2.json` | 7,020 | Has `ready` field instead of `value`; fields: timestamp, ready, namespace, service, type |
| `svcLatencyQuantilesMeasurement-cluster-density-v2.json` | ? | Quantile summaries |
| `podLatencyQuantilesMeasurement-cluster-density-v2.json` | ? | Quantile summaries |

Needs either adaptation of `pod_latency_stats.py` or a new script. The svcLatency format uses `ready` (likely a timestamp) rather than explicit latency values.

### Other metrics (counts, rates, status)

| File | Records | Notes |
|------|---------|-------|
| `schedulerThroughput.json` | 10 | Single time-series, no labels |
| `prometheus-ingestionrate.json` | ? | Prometheus self-monitoring |
| `prometheus-timeseriestotal.json` | ? | Prometheus cardinality |
| `configmapCount.json` | ? | Resource counts |
| `deploymentCount.json` | ? | Resource counts |
| `namespaceCount.json` | ? | Resource counts |
| `podStatusCount.json` | ? | Pod status distribution |
| `routeCount.json` | ? | Resource counts |
| `secretCount.json` | ? | Resource counts |
| `serviceCount.json` | ? | Resource counts |
| `nodeRoles.json` | ? | Node role metadata |
| `nodeStatus.json` | ? | Node condition statuses |
| `etcdVersion.json` | ? | Etcd version metadata |
| `alert.json` | ? | Fired alerts during test |
| `jobSummary.json` | ? | Kube-burner job summary |

## Suggested Next Steps

1. ~~Run `container_stats.py` on network/crio/kubelet files~~ ✅ Done
2. ~~Analyze API latency + request rate files~~ ✅ Done — events SLO violation found
3. Inspect etcd files - reconcile raw vs non-raw vs DurationSeconds variants
4. Adapt analysis for `svcLatencyMeasurement` format (ready field instead of value)
5. Write combined cross-metric report tying CPU/memory/latency/etcd findings together
