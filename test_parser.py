"""Tests for parser.py — covers argument classification, direct file resolution,
group-by discovery, label filtering, and end-to-end CLI invocations."""

import os
import subprocess
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(__file__))
import parser as P

TESTDATA_DIR = os.path.join(os.path.dirname(__file__), "testdata")
METRICS_DIR = os.path.join(TESTDATA_DIR,
    "collected-metrics-2178a534-fce2-4d66-839b-d874c01dc630")
CPU_JSON = os.path.join(METRICS_DIR, "containerCPU.json")
CPU_GZ = os.path.join(METRICS_DIR, "containerCPU.json.gz")


# ---------- _resolve_direct_file ----------

class TestResolveDirectFile:
    def test_existing_json(self):
        assert P._resolve_direct_file(CPU_JSON) == CPU_JSON

    def test_existing_gz(self):
        assert P._resolve_direct_file(CPU_GZ) == CPU_GZ

    def test_without_extension(self):
        base = os.path.join(METRICS_DIR, "containerCPU")
        resolved = P._resolve_direct_file(base)
        assert resolved is not None
        assert resolved.endswith(".json")

    def test_nonexistent(self):
        assert P._resolve_direct_file("/no/such/file.json") is None

    def test_nonexistent_no_extension(self):
        assert P._resolve_direct_file("/no/such/file") is None


# ---------- _discover_group_values ----------

class TestDiscoverGroupValues:
    def test_returns_values(self):
        vals = P._discover_group_values(CPU_JSON, "container", top_n=5)
        assert len(vals) > 0
        assert len(vals) <= 5
        for name, count in vals:
            assert isinstance(name, str)
            assert isinstance(count, int)
            assert count > 0

    def test_sorted_by_count_descending(self):
        vals = P._discover_group_values(CPU_JSON, "container", top_n=10)
        counts = [c for _, c in vals]
        assert counts == sorted(counts, reverse=True)

    def test_respects_top_n(self):
        vals_2 = P._discover_group_values(CPU_JSON, "container", top_n=2)
        vals_5 = P._discover_group_values(CPU_JSON, "container", top_n=5)
        assert len(vals_2) == 2
        assert len(vals_5) == 5

    def test_nonexistent_key(self):
        vals = P._discover_group_values(CPU_JSON, "nonexistent_key_xyz", top_n=5)
        assert len(vals) <= 1  # may have "<missing>" bucket


# ---------- match_label_filters ----------

class TestMatchLabelFilters:
    def test_no_filters(self):
        assert P.match_label_filters({"labels": {"a": "1"}}, None)
        assert P.match_label_filters({"labels": {"a": "1"}}, {})

    def test_matching(self):
        assert P.match_label_filters({"labels": {"container": "foo"}},
                                     {"container": "foo"})

    def test_not_matching(self):
        assert not P.match_label_filters({"labels": {"container": "foo"}},
                                         {"container": "bar"})

    def test_missing_key(self):
        assert not P.match_label_filters({"labels": {}},
                                         {"container": "foo"})


# ---------- _compute_group_stats ----------

class TestComputeGroupStats:
    def test_basic(self):
        entries = [
            {"value": 1.0, "labels": {"k": "a"}},
            {"value": 2.0, "labels": {"k": "a"}},
            {"value": 10.0, "labels": {"k": "b"}},
        ]
        results = P._compute_group_stats(entries, "k")
        assert len(results) == 2
        names = {r[0] for r in results}
        assert names == {"a", "b"}

    def test_top_n(self):
        entries = [
            {"value": 1.0, "labels": {"k": "a"}},
            {"value": 2.0, "labels": {"k": "b"}},
            {"value": 3.0, "labels": {"k": "c"}},
        ]
        results = P._compute_group_stats(entries, "k", top_n=2)
        assert len(results) == 2

    def test_missing_key(self):
        entries = [{"value": 1.0, "labels": {"other": "x"}}]
        results = P._compute_group_stats(entries, "nonexistent")
        assert results == []


# ---------- _print_group_stats_table ----------

class TestPrintGroupStatsTable:
    def test_large_values_use_scientific_notation(self, capsys):
        entries = [
            {"value": 3.49e+09, "labels": {"id": "/kubepods.slice"}},
            {"value": 4.09e+09, "labels": {"id": "/kubepods.slice"}},
            {"value": 6.2e+08, "labels": {"id": "/system.slice"}},
            {"value": 8.3e+08, "labels": {"id": "/system.slice"}},
        ]
        results = P._compute_group_stats(entries, "id")
        P._print_group_stats_table(results, "id", "cgroupMemoryRSS", len(entries))
        out = capsys.readouterr().out
        assert "e+09" in out or "e+08" in out
        for line in out.splitlines():
            assert len(line) <= 140, f"Line too wide ({len(line)} chars): {line}"

    def test_small_values_no_scientific(self, capsys):
        entries = [
            {"value": 0.025, "labels": {"k": "a"}},
            {"value": 0.030, "labels": {"k": "a"}},
        ]
        results = P._compute_group_stats(entries, "k")
        P._print_group_stats_table(results, "k", "containerCPU", len(entries))
        out = capsys.readouterr().out
        assert "e+" not in out


# ---------- _compute_stats ----------

class TestComputeStats:
    def test_basic_stats(self):
        vals = sorted([1.0, 2.0, 3.0, 4.0, 5.0])
        stats = P._compute_stats(vals)
        assert stats["n"] == 5
        assert stats["min"] == 1.0
        assert stats["max"] == 5.0
        assert stats["avg"] == 3.0
        assert "p50" in stats
        assert "p90" in stats
        assert "p99" in stats
        assert "stdev" in stats
        assert "cv" in stats

    def test_single_value(self):
        stats = P._compute_stats([42.0])
        assert stats["n"] == 1
        assert stats["avg"] == 42.0
        assert stats["stdev"] == 0.0


# ---------- load_generic_metrics ----------

class TestLoadGenericMetrics:
    def test_returns_values(self):
        vals = P.load_generic_metrics(CPU_JSON)
        assert len(vals) > 0
        assert all(isinstance(v, (int, float)) for v in vals)

    def test_with_label_filter(self):
        all_vals = P.load_generic_metrics(CPU_JSON)
        filtered = P.load_generic_metrics(CPU_JSON,
            label_filters={"container": "kube-rbac-proxy"})
        assert len(filtered) < len(all_vals)
        assert len(filtered) > 0

    def test_return_entries(self):
        entries = P.load_generic_metrics(CPU_JSON, return_entries=True)
        assert len(entries) > 0
        assert "value" in entries[0]
        assert "timestamp" in entries[0]


# ---------- CLI end-to-end ----------

def _run_parser(*args):
    """Run parser.py as subprocess, return (returncode, stdout, stderr)."""
    cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "parser.py")] + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return result.returncode, result.stdout, result.stderr


class TestCLIDirectFile:
    def test_direct_json_path(self):
        rc, out, err = _run_parser(CPU_JSON, "--no-visuals")
        assert rc == 0
        assert "METRICS: containerCPU" in out

    def test_direct_path_without_extension(self):
        base = os.path.join(METRICS_DIR, "containerCPU")
        rc, out, err = _run_parser(base, "--no-visuals")
        assert rc == 0
        assert "METRICS: containerCPU" in out

    def test_direct_gz_path(self):
        rc, out, err = _run_parser(CPU_GZ, "--no-visuals")
        assert rc == 0
        assert "METRICS:" in out


class TestCLIUuidMode:
    def test_uuid_latency(self):
        rc, out, err = _run_parser("2178a534", "--no-visuals")
        assert rc == 0

    def test_uuid_metrics(self):
        rc, out, err = _run_parser("2178a534", "metrics", "containerCPU",
                                   "--no-visuals")
        assert rc == 0
        assert "METRICS: containerCPU" in out


class TestCLIGroupBy:
    def test_group_by_shows_table(self):
        rc, out, err = _run_parser(CPU_JSON, "--group-by", "container",
                                   "--no-visuals", "--top-labels", "3")
        assert rc == 0
        assert "GROUP BY [container]" in out

    def test_group_by_shows_per_group_plots(self):
        rc, out, err = _run_parser(CPU_JSON, "--group-by", "container",
                                   "--top-labels", "2")
        assert rc == 0
        assert "[container=" in out
        group_headers = [l for l in out.splitlines() if "[container=" in l]
        assert len(group_headers) >= 2

    def test_group_by_no_visuals_skips_per_group_plots(self):
        rc, out, err = _run_parser(CPU_JSON, "--group-by", "container",
                                   "--no-visuals", "--top-labels", "2")
        assert rc == 0
        assert "GROUP BY [container]" in out
        assert "Histogram" not in out
        assert "CDF" not in out

    def test_group_by_invalid_key(self):
        rc, out, err = _run_parser(CPU_JSON, "--group-by", "nonexistent_xyz",
                                   "--no-visuals")
        assert rc == 0  # should not crash


class TestCLILabelFilter:
    def test_label_filter(self):
        rc, out, err = _run_parser(CPU_JSON, "-l", "container=kube-rbac-proxy",
                                   "--no-visuals")
        assert rc == 0
        assert "N = 210" in out


class TestCLINoArgs:
    def test_no_args_shows_help(self):
        rc, out, err = _run_parser()
        assert rc != 0
