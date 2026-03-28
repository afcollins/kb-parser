# podLatencyMeasurement-cluster-density-v2.json Analysis Report

**Source**: OpenShift 4.23 cluster-density-v2 test, 120-node AWS cluster
**Dataset**: 14,040 records, each with 6 latency fields (ms)

## Bimodal Confirmation

**containersReadyLatency / podReadyLatency** (identical values) have a clear bimodal distribution:

| Band | N | % | Mean | Range | CV |
|------|---|---|------|-------|-----|
| Band 1 (fast) | 9,682 | 69.0% | 1,518 ms | 0-5,000 ms | 47.8% |
| Band 2 (slow) | 4,358 | 31.0% | 12,756 ms | 11,000-15,000 ms | 5.3% |

- **Separation quality: 8.57** (well above 2.0 threshold)
- **6,000 ms gap** between bands (nothing falls in 5,000-11,000 ms)
- Optimal split point: 8,000 ms

Raw value distribution:
- 1s: 37%, 2s: 25%, 3s: 4.3%, 4s: 1.1% (fast band)
- 12s: 11.4%, 13s: 15.7%, 14s: 3.8% (slow band)

## Other Latency Fields

| Field | Pattern | Detail |
|-------|---------|--------|
| schedulingLatency | Sparse tail | 96.8% are 0ms, 3.2% are 385-1028ms |
| initializedLatency | Binary | 96.6% at 0ms, 3.4% at 1000ms |
| containersStartedLatency | Unimodal normal | mean=1318ms, CV=25.3%, no bimodality |
| readyToStartContainersLatency | Discrete | 61.6% at 1s, 35.4% at 2s |

## Recommended Percentiles for Change Detection

For **containersReadyLatency/podReadyLatency**:

| Percentile | Value | Role | Why |
|------------|-------|------|-----|
| P20 | 1,000 ms | Band 1 center | Stable plateau; any shift = fast-path behavior changed |
| P51 | 2,000 ms | Band 1 upper | Captures proportion of 1s vs 2s pods in fast band |
| P68 | 4,000 ms | Transition boundary | Right at fast-band edge; first to move if band ratio shifts |
| P69 | 11,000 ms | Transition boundary | First slow-band value; detects if gap narrows |
| P75 | 12,000 ms | Band 2 lower center | Stable within slow band |
| P88 | 13,000 ms | Band 2 upper center | Shifts here = slow-band behavior changed |
| P98 | 14,000 ms | Band 2 tail | Detects slow-band expansion |

### Critical Pair: P68/P69

These straddle the 6,000ms gap. If the bimodal split ratio changes (more pods in slow band), P68 jumps from ~4,000 to ~11,000+. This single percentile crossing the gap is the most sensitive indicator of a regime change.

### Band-Specific Monitoring

- **Fast band stability**: P20, P51 (both on stable plateaus)
- **Slow band stability**: P75, P88 (both on stable plateaus)
- **Band ratio shift**: P68-P69 (the transition)

## Per-Field Statistics (ms)

| Field | Mean | Std | CV% | P5 | P25 | Median | P75 | P95 | P99 | Max |
|-------|------|-----|-----|-----|-----|--------|-----|-----|-----|-----|
| schedulingLatency | 27.0 | 140.5 | 520.7% | 0 | 0 | 0 | 0 | 0 | 902 | 1028 |
| initializedLatency | 34.0 | 181.3 | 532.7% | 0 | 0 | 0 | 0 | 0 | 1000 | 1000 |
| containersReadyLatency | 5006.3 | 5247.8 | 104.8% | 1000 | 1000 | 2000 | 12000 | 13000 | 14000 | 15000 |
| podReadyLatency | 5006.3 | 5247.8 | 104.8% | 1000 | 1000 | 2000 | 12000 | 13000 | 14000 | 15000 |
| containersStartedLatency | 1317.8 | 332.8 | 25.3% | 783 | 1058 | 1319 | 1568 | 1867 | 2032 | 3249 |
| readyToStartContainersLatency | 1327.6 | 528.4 | 39.8% | 1000 | 1000 | 1000 | 2000 | 2000 | 2000 | 3000 |

## Validating Percentile Bands Across Experiments

To confirm these percentile recommendations are reliable and not an artifact of a single run, apply the following cross-validation strategy:

### 1. Multi-Run Stability Check

Run `pod_latency_stats.py` against podLatencyMeasurement JSON files from multiple cluster-density-v2 runs. For each run, record:
- The optimal split point (should stay near 8,000 ms)
- The band proportions (Band 1 % vs Band 2 %)
- The gap width (should remain > 4,000 ms)
- The separation quality score (should stay > 2.0)

If these are stable across runs, the bimodal structure is intrinsic to the workload, not noise.

### 2. Percentile Bootstrap Confidence Intervals

Resample (with replacement) within a single run's data, compute the recommended percentiles on each resample, and check how much they vary. Tight confidence intervals = reliable percentile. Wide intervals = the percentile sits near a transition and is sensitive to sampling noise.

### 3. Cross-Cluster / Cross-Version Comparison

Compare runs across:
- Different node counts (e.g., 24-node vs 120-node)
- Different OCP versions (4.22 vs 4.23)
- Different cloud providers / instance types

The bimodal split may shift (different band ratio), but if the gap persists and the recommended percentiles still fall on stable plateaus, the monitoring strategy is robust.

### 4. Sensitivity Simulation

Artificially shift 5-10% of pods from Band 1 to Band 2 (or vice versa) and observe which percentiles change most. This validates that P68/P69 are the most sensitive to ratio changes, and that P20/P88 are stable within their bands.

## CSV Output for Cross-Run Comparison

`pod_latency_stats.py` writes a `podLatency-percentile-bands.csv` alongside the input file. Each row is one latency field from one run. Columns include:

- **Run metadata**: file, uuid, jobName, ocp_version, total_records
- **Global stats**: mean, std, cv, min, max, unique_values
- **Percentiles**: P1, P2, P5, P10, P20, P25, P33, P50, P51, P65, P68, P69, P75, P80, P88, P90, P95, P98, P99
- **Bimodal split**: split_point, gap, separation_quality
- **Per-band stats**: band{1,2}\_{n, pct, mean, std, min, max, cv}
- **Per-band percentiles**: band{1,2}\_P{1..99}
- **CDF transitions**: transition\_{1..5}\_{percentile, delta\_ms, value\_ms}

To combine across runs, concatenate CSVs (skip duplicate headers) or pass multiple JSON files:

```bash
python3 pod_latency_stats.py run1/podLatency*.json run2/podLatency*.json run3/podLatency*.json
```

Then use the combined CSV to track trends in split_point, separation_quality, band proportions, and recommended percentile values across OCP versions, node counts, or cloud providers.
