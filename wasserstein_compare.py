#!/usr/bin/env python3
"""
Compare two metric distributions using the Wasserstein (Earth Mover's) distance.

Loads two JSON metric files in kube-burner {timestamp, labels, value} format,
applies optional label filters, and computes the Wasserstein distance between
the resulting value distributions. Generates overlaid histogram and CDF plots
as PNG files.

Usage:
    # Compare same file with itself (distance should be ~0)
    python3 wasserstein_compare.py file.json file.json

    # Compare two files with shared label filter
    python3 wasserstein_compare.py file1.json file2.json --filter container=prometheus

    # Compare with per-file filters
    python3 wasserstein_compare.py file1.json file2.json \
        --filter1 container=prometheus --filter2 container=kube-apiserver

    # Pairwise comparison across a shared label dimension
    python3 wasserstein_compare.py file1.json file2.json --pairwise container

    # Run built-in unit tests with synthetic data
    python3 wasserstein_compare.py --self-test

    # Normalize values before comparing (z-score)
    python3 wasserstein_compare.py file1.json file2.json --normalize zscore
"""
import argparse
import json
import os
import statistics
import sys
import unittest

import numpy as np
import seaborn as sns
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from scipy.stats import wasserstein_distance


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_values(filepath, label_filters=None):
    """Load a metric JSON file and return filtered values.

    Args:
        filepath: Path to a JSON file with [{timestamp, labels, value}, ...].
        label_filters: Optional dict of {label_key: label_value} to filter on.

    Returns:
        List of float values matching the filters.
    """
    with open(filepath) as f:
        data = json.load(f)

    values = []
    for entry in data:
        if label_filters:
            labels = entry.get('labels', {})
            if not all(labels.get(k) == v for k, v in label_filters.items()):
                continue
        values.append(float(entry['value']))
    return values


def parse_filter_string(filter_str):
    """Parse 'key1=val1,key2=val2' into a dict."""
    if not filter_str:
        return None
    result = {}
    for pair in filter_str.split(','):
        k, _, v = pair.partition('=')
        k, v = k.strip(), v.strip()
        if k and v:
            result[k] = v
    return result or None


def discover_label_values(filepath, label_key, label_filters=None):
    """Return the set of unique values for a given label key, after applying filters."""
    with open(filepath) as f:
        data = json.load(f)
    values = set()
    for entry in data:
        labels = entry.get('labels', {})
        if label_filters:
            if not all(labels.get(k) == v for k, v in label_filters.items()):
                continue
        v = labels.get(label_key)
        if v is not None:
            values.add(v)
    return values


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def normalize_zscore(values):
    """Normalize values to z-scores (mean=0, std=1).

    Returns the normalized list plus (mean, std) used, so results can be
    interpreted. If std is 0 (constant data), returns zeros.
    """
    if len(values) < 2:
        return values, (0.0, 1.0)
    mean = statistics.mean(values)
    std = statistics.pstdev(values)
    if std == 0:
        return [0.0] * len(values), (mean, std)
    return [(v - mean) / std for v in values], (mean, std)


def normalize_minmax(values):
    """Normalize values to [0, 1] range.

    Returns the normalized list plus (min, max) used.
    """
    if not values:
        return values, (0.0, 1.0)
    lo, hi = min(values), max(values)
    if lo == hi:
        return [0.5] * len(values), (lo, hi)
    return [(v - lo) / (hi - lo) for v in values], (lo, hi)


def normalize_rank(values):
    """Normalize by rank (values replaced by their percentile rank 0-1).

    Returns the normalized list plus None (no parameters to report).
    """
    if not values:
        return values, None
    n = len(values)
    order = sorted(range(n), key=lambda i: values[i])
    ranked = [0.0] * n
    for rank, idx in enumerate(order):
        ranked[idx] = rank / (n - 1) if n > 1 else 0.5
    return ranked, None


NORMALIZERS = {
    'zscore': normalize_zscore,
    'minmax': normalize_minmax,
    'rank': normalize_rank,
}


def apply_normalization(values, method):
    """Apply the named normalization method. Returns (normalized_values, params)."""
    if method is None or method == 'none':
        return values, None
    fn = NORMALIZERS.get(method)
    if fn is None:
        raise ValueError(f"Unknown normalization method: {method!r}. "
                         f"Choose from: {', '.join(NORMALIZERS)}")
    return fn(values)


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------

def compare_distributions(vals_a, vals_b, label_a='A', label_b='B',
                          normalize=None, output_dir='.', tag=''):
    """Compute Wasserstein distance and generate plots.

    Args:
        vals_a, vals_b: Lists of float values.
        label_a, label_b: Display names for the two distributions.
        normalize: Normalization method name or None.
        output_dir: Directory for PNG output.
        tag: Optional filename prefix/tag.

    Returns:
        dict with distance, sizes, and normalization info.
    """
    norm_a, params_a = apply_normalization(vals_a, normalize)
    norm_b, params_b = apply_normalization(vals_b, normalize)

    dist = wasserstein_distance(norm_a, norm_b)

    result = {
        'wasserstein_distance': dist,
        'n_a': len(vals_a),
        'n_b': len(vals_b),
        'normalize': normalize or 'none',
        'label_a': label_a,
        'label_b': label_b,
    }
    if params_a is not None:
        result['norm_params_a'] = params_a
    if params_b is not None:
        result['norm_params_b'] = params_b

    # --- Plots ---
    safe_tag = tag.replace('/', '_').replace(' ', '_') if tag else 'compare'
    os.makedirs(output_dir, exist_ok=True)

    sns.set_theme(style='whitegrid', font_scale=0.9)

    # Overlaid histogram
    fig, ax = plt.subplots(figsize=(5, 3))
    ax.hist(norm_a, bins=50, alpha=0.5, label=label_a, density=True)
    ax.hist(norm_b, bins=50, alpha=0.5, label=label_b, density=True)
    ax.set_xlabel('Value' if not normalize else f'Value ({normalize})')
    ax.set_ylabel('Density')
    ax.set_title(f'W={dist:.4f}')
    ax.legend(fontsize=7)
    fig.tight_layout()
    hist_path = os.path.join(output_dir, f'{safe_tag}_hist.png')
    fig.savefig(hist_path, dpi=150)
    plt.close(fig)
    result['hist_png'] = hist_path

    # Overlaid CDF
    fig, ax = plt.subplots(figsize=(5, 3))
    sorted_a = np.sort(norm_a)
    sorted_b = np.sort(norm_b)
    ax.plot(sorted_a, np.linspace(0, 1, len(sorted_a)), label=label_a)
    ax.plot(sorted_b, np.linspace(0, 1, len(sorted_b)), label=label_b)
    ax.set_xlabel('Value' if not normalize else f'Value ({normalize})')
    ax.set_ylabel('CDF')
    ax.set_title(f'W={dist:.4f}')
    ax.legend(fontsize=7)
    fig.tight_layout()
    cdf_path = os.path.join(output_dir, f'{safe_tag}_cdf.png')
    fig.savefig(cdf_path, dpi=150)
    plt.close(fig)
    result['cdf_png'] = cdf_path

    return result


def print_result(result):
    """Print comparison result to stdout."""
    print(f"\n{'=' * 70}")
    print(f"  {result['label_a']}  vs  {result['label_b']}")
    print(f"  N: {result['n_a']} vs {result['n_b']}")
    print(f"  Normalization: {result['normalize']}")
    print(f"  Wasserstein distance: {result['wasserstein_distance']:.6f}")
    if 'norm_params_a' in result:
        print(f"  Norm params A: {result['norm_params_a']}")
    if 'norm_params_b' in result:
        print(f"  Norm params B: {result['norm_params_b']}")
    print(f"  Histogram: {result.get('hist_png', 'N/A')}")
    print(f"  CDF:       {result.get('cdf_png', 'N/A')}")
    print(f"{'=' * 70}")


# ---------------------------------------------------------------------------
# Pairwise comparison
# ---------------------------------------------------------------------------

def pairwise_compare(filepath_a, filepath_b, label_key,
                     filter_a=None, filter_b=None,
                     normalize=None, output_dir='.'):
    """Compare distributions for each shared label value."""
    vals_a_set = discover_label_values(filepath_a, label_key, filter_a)
    vals_b_set = discover_label_values(filepath_b, label_key, filter_b)
    shared = sorted(vals_a_set & vals_b_set)

    if not shared:
        print(f"No shared values for label '{label_key}' between the two files.")
        return []

    print(f"\nPairwise comparison on '{label_key}': {len(shared)} shared values")
    results = []
    for lv in shared:
        filt_a = dict(filter_a or {}, **{label_key: lv})
        filt_b = dict(filter_b or {}, **{label_key: lv})
        va = load_values(filepath_a, filt_a)
        vb = load_values(filepath_b, filt_b)
        if len(va) < 2 or len(vb) < 2:
            continue
        name_a = os.path.basename(filepath_a)
        name_b = os.path.basename(filepath_b)
        r = compare_distributions(
            va, vb,
            label_a=f'{name_a}:{lv}',
            label_b=f'{name_b}:{lv}',
            normalize=normalize,
            output_dir=output_dir,
            tag=f'pairwise_{label_key}_{lv}',
        )
        results.append(r)
        print(f"  {lv:50s}  W={r['wasserstein_distance']:.6f}  "
              f"(N: {r['n_a']} vs {r['n_b']})")

    # Summary
    if results:
        dists = [r['wasserstein_distance'] for r in results]
        print(f"\n  Summary: min={min(dists):.6f}  median={statistics.median(dists):.6f}  "
              f"max={max(dists):.6f}  mean={statistics.mean(dists):.6f}")
    return results


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

class SyntheticTests(unittest.TestCase):
    """Unit tests using synthetic data in {timestamp, labels, value} format."""

    @staticmethod
    def _make_records(values, labels=None):
        """Create records in kube-burner format."""
        return [{'timestamp': 1000.0 + i, 'labels': labels or {}, 'value': v}
                for i, v in enumerate(values)]

    def test_identical_distributions(self):
        """Distance of a distribution with itself should be 0."""
        vals = list(np.random.normal(5.0, 1.0, 500))
        d = wasserstein_distance(vals, vals)
        self.assertAlmostEqual(d, 0.0, places=10)

    def test_shifted_distribution(self):
        """Shifting a distribution by k should give distance ~k."""
        np.random.seed(42)
        base = list(np.random.normal(0, 1, 1000))
        shifted = [v + 3.0 for v in base]
        d = wasserstein_distance(base, shifted)
        self.assertAlmostEqual(d, 3.0, places=1)

    def test_different_units_raw(self):
        """CPU (0-10 cores) vs Memory (1e8-1e10 bytes) should give huge distance."""
        np.random.seed(42)
        cpu = list(np.random.uniform(0.1, 10.0, 500))
        mem = list(np.random.uniform(1e8, 1e10, 500))
        d = wasserstein_distance(cpu, mem)
        self.assertGreater(d, 1e6, "Raw distance between CPU and memory should be huge")

    def test_different_units_normalized(self):
        """After z-score normalization, CPU vs Memory distance becomes small."""
        np.random.seed(42)
        cpu = list(np.random.uniform(0.1, 10.0, 500))
        mem = list(np.random.uniform(1e8, 1e10, 500))
        cpu_z, _ = normalize_zscore(cpu)
        mem_z, _ = normalize_zscore(mem)
        d = wasserstein_distance(cpu_z, mem_z)
        # Both are uniform distributions, so normalized they should be similar
        self.assertLess(d, 0.5, "Normalized uniform distributions should be close")

    def test_load_and_filter(self):
        """Verify load_values respects label filters."""
        import tempfile
        records = (
            self._make_records([1.0, 2.0, 3.0], {'container': 'a'}) +
            self._make_records([10.0, 20.0], {'container': 'b'})
        )
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(records, f)
            path = f.name
        try:
            all_vals = load_values(path)
            self.assertEqual(len(all_vals), 5)
            a_vals = load_values(path, {'container': 'a'})
            self.assertEqual(a_vals, [1.0, 2.0, 3.0])
            b_vals = load_values(path, {'container': 'b'})
            self.assertEqual(b_vals, [10.0, 20.0])
        finally:
            os.unlink(path)

    def test_normalize_zscore(self):
        """Z-score normalization should produce mean≈0, std≈1."""
        vals = list(np.random.normal(100, 15, 1000))
        normed, (mean, std) = normalize_zscore(vals)
        self.assertAlmostEqual(statistics.mean(normed), 0.0, places=5)
        self.assertAlmostEqual(statistics.pstdev(normed), 1.0, places=5)

    def test_normalize_minmax(self):
        """Min-max normalization should produce values in [0, 1]."""
        vals = [3.0, 7.0, 1.0, 9.0, 5.0]
        normed, (lo, hi) = normalize_minmax(vals)
        self.assertAlmostEqual(min(normed), 0.0)
        self.assertAlmostEqual(max(normed), 1.0)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(
        description='Compare two metric distributions using Wasserstein distance.')
    p.add_argument('files', nargs='*', help='Two JSON metric files to compare')
    p.add_argument('--filter', dest='shared_filter', default=None,
                   help='Label filter applied to both files (key=val,key2=val2)')
    p.add_argument('--filter1', default=None,
                   help='Label filter for file 1 only')
    p.add_argument('--filter2', default=None,
                   help='Label filter for file 2 only')
    p.add_argument('--normalize', default=None,
                   choices=['none', 'zscore', 'minmax', 'rank'],
                   help='Normalization method (default: none)')
    p.add_argument('--pairwise', default=None, metavar='LABEL_KEY',
                   help='Run pairwise comparison across shared values of this label')
    p.add_argument('--output-dir', default='.',
                   help='Directory for PNG output (default: current dir)')
    p.add_argument('--self-test', action='store_true',
                   help='Run built-in unit tests')
    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.self_test:
        # Run unit tests
        sys.argv = [sys.argv[0]]  # hide args from unittest
        unittest.main(module=__name__, exit=True, verbosity=2)
        return

    if len(args.files) != 2:
        parser.error('Exactly two JSON files are required (or use --self-test)')

    file_a, file_b = args.files

    # Build per-file filters
    shared = parse_filter_string(args.shared_filter)
    f1 = parse_filter_string(args.filter1)
    f2 = parse_filter_string(args.filter2)
    filter_a = {**(shared or {}), **(f1 or {})}  or None
    filter_b = {**(shared or {}), **(f2 or {})}  or None

    if args.pairwise:
        pairwise_compare(file_a, file_b, args.pairwise,
                         filter_a=filter_a, filter_b=filter_b,
                         normalize=args.normalize,
                         output_dir=args.output_dir)
        return

    # Load
    vals_a = load_values(file_a, filter_a)
    vals_b = load_values(file_b, filter_b)

    if not vals_a:
        print(f"Error: No values loaded from {file_a} with filters {filter_a}")
        sys.exit(1)
    if not vals_b:
        print(f"Error: No values loaded from {file_b} with filters {filter_b}")
        sys.exit(1)

    name_a = os.path.basename(file_a)
    name_b = os.path.basename(file_b)
    if filter_a:
        name_a += f" [{','.join(f'{k}={v}' for k,v in filter_a.items())}]"
    if filter_b:
        name_b += f" [{','.join(f'{k}={v}' for k,v in filter_b.items())}]"

    tag = f"{os.path.splitext(os.path.basename(file_a))[0]}_vs_{os.path.splitext(os.path.basename(file_b))[0]}"

    result = compare_distributions(
        vals_a, vals_b,
        label_a=name_a, label_b=name_b,
        normalize=args.normalize,
        output_dir=args.output_dir,
        tag=tag,
    )
    print_result(result)


if __name__ == '__main__':
    main()
