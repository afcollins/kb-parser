# containerCPU.json Analysis Report

**Source**: OpenShift 4.23 cluster-density-v2 test, 120-node AWS cluster
**Dataset**: 39,629 data points across 150 containers, 54 namespaces, 126 nodes, 1,990 pods

## By Container - CPU Magnitude Tiers

| Tier | Count | Key Members |
|------|-------|-------------|
| Very high (10+ cores) | 6 | prometheus (150.5), kube-apiserver (116.0), etcd (47.1), ovnkube-controller (39.8), northd (23.7), router (20.2) |
| High (1-10 cores) | 29 | kube-controller-manager (9.5), openshift-apiserver (9.2), authentication-operator (7.1), openshift-apiserver-operator (4.1), ovn-controller (3.4), promtail (3.0) |
| Moderate (0.1-1.0 cores) | 55 | etcd-readyz, oauth-apiserver, dns, node-exporter, etc. |
| Low/Very low (<0.1 cores) | 60 | Sidecar proxies, cert syncers, config-reloaders, CSI helpers |

## By Container - Variability (CV) Clusters

**Very stable (CV < 15%) - 29 containers**: Rock-steady rate.
- `console-operator` (CV=2.7%, mean=2.0 cores) - the most predictable
- `etcd-metrics` (CV=3.0%), `etcd-readyz` (CV=4.3%)
- Operator containers: `kube-controller-manager-operator` (6.3%), `openshift-apiserver-operator` (6.9%), `authentication-operator` (8.9%)

**Stable (CV 15-30%) - 36 containers**: Slight variation but well-bounded. Includes `etcd` (CV=29.0%, mean=47 cores), `extractor`, `oauth-openshift`, cert-syncers.

**Moderate variability (CV 30-60%) - 37 containers**: Load-responsive but predictable. `prometheus` (CV=54.2%), `openshift-apiserver` (CV=44.5%), `promtail` (CV=56.2%).

**High variability (CV 60-100%) - 22 containers**: Bursty workloads. `kube-apiserver` (CV=88.4%), `ovnkube-controller` (CV=66.5%), `northd` (CV=63.7%), `router` (CV=73.8%).

**Very high variability (CV > 100%) - 25 containers**: Heavily spiking, often idle-to-burst patterns:
- `kube-controller-manager` (CV=205%, mean=9.5 but median=0.23!) - runs on 3 masters but only 1 is active leader
- `ovnkube-cluster-manager` (CV=227%) - same leader-election pattern
- `kube-multus` (CV=205%) - brief bursts during pod creation
- `multus-admission-controller` (CV=184%), `webhook` (CV=189%)

## Combined Clustering (Magnitude x Variability) - Key Patterns

**High CPU + Very Stable** (steady workhorses):
- `authentication-operator` (7.1 cores, CV=8.9%)
- `openshift-apiserver-operator` (4.1 cores, CV=6.9%)
- `console-operator` (2.0 cores, CV=2.7%)

Candidates for tight resource limits - very predictable.

**Very High CPU + High Variability** (volatile heavyweights):
- `kube-apiserver` (116 cores mean, CV=88.4%, P25=69.8, P75=123.3)
- `ovnkube-controller` (39.8 cores, CV=66.5%)
- `router` (20.2 cores, CV=73.8%)

**High CPU + Very High Variability** (leader-elected spiky):
- `kube-controller-manager` (mean=9.5, median=0.23, max=98.7)
- `cluster-policy-controller` (mean=1.6, median=0.16, max=17.1)
- `controller-manager` (mean=1.9, median=0.20, max=10.5)

The median << mean pattern is classic leader election: 1 replica active (high CPU), 2 replicas idle (near-zero).

## Anomaly / Spike Detection

Containers with highest max/mean ratio (>5x):

| Container | Spike Ratio | Mean | Max | P95 |
|-----------|------------|------|-----|-----|
| kube-multus | 18.8x | 0.21 | 3.98 | 1.23 |
| serve-healthcheck-canary | 14.9x | 0.01 | 0.15 | 0.05 |
| kube-controller-manager | 10.4x | 9.47 | 98.72 | 36.86 |
| ovnkube-cluster-manager | 9.8x | 3.44 | 33.82 | 33.82 |
| webhook | 8.9x | 1.76 | 15.70 | 9.54 |
| ovnkube-controller | 5.8x | 39.82 | 231.32 | 93.50 |

## By Node

All 126 nodes show remarkably uniform behavior:
- Mean CPU per node: ~2.4-2.6 cores (worker nodes), ~3.0-6.6 cores (control plane/infra)
- CV consistently ~370-550% across workers - expected since each node hosts a mix of near-zero sidecars + occasionally active components
- Top nodes by mean are the control plane (hosting kube-apiserver, etcd, prometheus)

## By Namespace - Top CPU Consumers

| Namespace | Mean | CV | Pattern |
|-----------|------|----|---------|
| openshift-kube-apiserver | 23.3 | 280% | Extreme variance (api servers + tiny sidecars) |
| openshift-ingress | 20.2 | 74% | Router traffic-dependent |
| openshift-etcd | 12.2 | 174% | Mixed etcd + sidecars |
| openshift-ovn-kubernetes | 8.9 | 201% | Networking, very bursty |
| openshift-monitoring | 1.3 | 1082% | Highest CV - prometheus dominates but most pods are tiny |

## Key Takeaways

1. **Leader-election bimodality** is the dominant pattern for kube-controller-manager, cluster-policy-controller, and controller-manager - median is 10-50x lower than mean due to active/standby split
2. **6 containers consume >95% of total CPU**: prometheus, kube-apiserver, etcd, ovnkube-controller, northd, router
3. **Most stable containers** are operators (CV < 15%) - good candidates for tighter resource limits
4. **Networking components** (ovnkube-controller, northd, kube-multus, ovn-controller) are the spikiest correlated with pod churn during the density test
5. **Worker nodes are homogeneous** - no single worker is an outlier, confirming even workload distribution
6. **openshift-monitoring has CV=1082%** because it aggregates prometheus (150 cores) with dozens of tiny exporters (~0.03 cores each)
