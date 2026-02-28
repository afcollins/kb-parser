#!/usr/bin/env python3
import argparse
from collections import Counter, defaultdict
from contextlib import contextmanager, redirect_stdout
import csv
from dataclasses import dataclass
import io
import datetime
import enum
import hashlib
import json
import math
import os
import random
import re
import statistics
import sys
import threading
import time
import logging


@dataclass
class RenderConfig:
    """Groups all display/render CLI flags so they can be passed as one object."""
    no_visuals: bool = False
    scatter:    bool = False
    source:     bool = False
    top_labels: int  = 10
    agg:        bool = False
    plotly:     bool = False
    no_hist:    bool = False
    no_cdf:     bool = False
# plotly.express for scatter with marginal distributions
# import plotly.express as px
# Attempt to import plotille for terminal visuals
try:
    import plotille
except ImportError:
    print("[!] Run 'pip install plotille' to enable terminal graphing.")
    plotille = None

# Attempt to import plotly for browser visuals
try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError:
    print("[!] Run 'pip install plotly' to enable interactive browser graphing.")
    go = None
    make_subplots = None

# Use orjson for faster JSON parsing of large metric files when available.
# orjson only provides loads()/dumps() (not load()/dump()), so it is used
# only in the hot file-read path; stdlib json handles cache writes.
try:
    import orjson as _orjson
    def _json_load(f):
        return _orjson.loads(f.read())
except ImportError:
    print("[!] Run 'pip install orjson' for faster JSON parsing of large metric files.")
    _orjson = None
    def _json_load(f):
        return json.load(f)

# Use ijson for streaming JSON parse on cold runs (lower peak memory).
try:
    import ijson as _ijson
except ImportError:
    print("[!] Run 'pip install ijson' for streaming JSON parse on cold runs (lower memory).")
    _ijson = None

# Use msgpack for binary cache files (faster reads/writes than JSON).
try:
    import msgpack as _msgpack
except ImportError:
    print("[!] Run 'pip install msgpack' for faster cache reads/writes.")
    _msgpack = None

# Use tqdm for progress bars on long streaming parse operations.
try:
    import tqdm as _tqdm_mod
except ImportError:
    print("[!] Run 'pip install tqdm' to enable progress bars during cold parse.")
    _tqdm_mod = None

# Logging verbosity — set from CLI args in __main__.
_verbose = False
_quiet   = False
LOG_FORMAT="%(asctime)s %(levelname)5s %(filename)s:%(lineno)-4d : %(message)s"

def _info(msg):
    """Diagnostic/timing print. Shown by default; suppressed by --quiet."""
    if not _quiet:
        logging.info(msg)

def _debug(msg):
    """Verbose detail. Only shown with --verbose."""
    if _verbose:
        logging.debug(msg)

@contextmanager
def _spinner(desc="", show_timer=True):
    """Show an animated tqdm spinner on stderr during a blocking operation.

    Falls back to a no-op when tqdm is unavailable, --quiet is active, or
    stderr is not a TTY (pipes/redirects), so no escape codes leak into output.

    show_timer=True  — background thread updates an elapsed counter every 1s.
                       Works when the GIL is released (pure-Python ops).
    show_timer=False — static label only; no thread started. Use when the
                       blocking work is inside a C extension that holds the GIL,
                       where a frozen timer would be misleading.
    """
    if _tqdm_mod is None or _quiet or not sys.stderr.isatty():
        yield
        return
    bar = _tqdm_mod.tqdm(
        total=0,
        desc=f"{desc} [0.0s]" if show_timer else desc,
        bar_format="{desc}",
        file=sys.stderr,
        leave=False,
    )
    if not show_timer:
        try:
            yield
        finally:
            bar.close()
        return
    start = time.perf_counter()
    done = threading.Event()
    def _refresh():
        while not done.wait(1.0):
            elapsed = time.perf_counter() - start
            bar.set_description(f"{desc} [{elapsed:.1f}s]")
            bar.refresh()
    t = threading.Thread(target=_refresh, daemon=True)
    t.start()
    try:
        yield
    finally:
        done.set()
        t.join()
        bar.close()

def _progress(iterable, desc="", unit=" entries"):
    """Wrap iterable with a tqdm progress bar when tqdm is available and not quiet.

    Falls back transparently to the raw iterable when tqdm is missing or --quiet
    is active. tqdm auto-disables when stderr is not a TTY (pipes, redirects).
    """
    if _tqdm_mod is None or _quiet:
        return iterable
    return _tqdm_mod.tqdm(
        iterable,
        desc=desc,
        unit=unit,
        unit_scale=True,    # e.g. 1.2M instead of 1200000
        mininterval=0.5,    # update at most every 0.5s — negligible overhead
        file=sys.stderr,
        leave=False,        # bar clears on completion; no permanent terminal line
    )

# --- 1. DYNAMIC COLUMN CONFIGURATION ---
# Percentiles we want to calculate
QUANTILES_HEADERS = [f"P{i:02d}" for i in range(5, 100, 5)]

# We define the order by finding 'job_took' and inserting the percentiles right after it
ORIGINAL_COLUMNS = [
    "OCP Version", "k-b version", "workers", "workload", "scheduler",
    "iterations", "podReplicas", "start time", "UUID",
    "sched_p99", "sched_max", "sched_avg", "sched_stddev",
    "ready_p99", "ready_max", "ready_avg",
    "end time", "percent", "duration", "cycles", "job_took",
    "Note", "worker reserved cores", "worker CPUs", "topologyPolicy", "overall duration",
    "qps burst", "CV"
]
EXPERIMENTAL_COLUMNS = [
    "max_pods_per_sec", "avg_pods_per_sec"
]

COLUMN_ORDER = ORIGINAL_COLUMNS + QUANTILES_HEADERS + EXPERIMENTAL_COLUMNS

# --- 2. CONFIGURATION ---
SUMMARY_FILENAME = "jobSummary.json"
OUTPUT_FILE = "kube-burner-ocp-final-report.csv"
DEFAULT_VAL = "-"

class LatencyType(enum.Enum):
    podReadyLatency               = "podReadyLatency"
    schedulingLatency             = "schedulingLatency"
    initializedLatency            = "initializedLatency"
    containersReadyLatency        = "containersReadyLatency"
    readyToStartContainersLatency = "readyToStartContainersLatency"

    def __str__(self): return self.value

def _resolve_latency_type(s):
    """Match s against LatencyType values by unambiguous prefix."""
    matches = [t for t in LatencyType if t.value.startswith(s)]
    if len(matches) == 1:
        return matches[0].value
    if len(matches) == 0:
        choices = ", ".join(t.value for t in LatencyType)
        raise argparse.ArgumentTypeError(
            f"unknown latency type {s!r}. Choices: {choices}"
        )
    raise argparse.ArgumentTypeError(
        f"ambiguous prefix {s!r} matches: {', '.join(t.value for t in matches)}"
    )


# --- 2. HELPER FUNCTIONS ---

def _parse_timestamp(ts_raw):
    """Parse ISO timestamp string (or epoch float) to datetime. Returns None on failure."""
    if isinstance(ts_raw, (int, float)):
        return datetime.datetime.fromtimestamp(ts_raw, tz=datetime.timezone.utc)
    try:
        return datetime.datetime.fromisoformat(str(ts_raw).replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


def get_pretty_step(total_range_ms):
    """Returns a clean step size (1s, 2s, 5s, 10s, etc.) based on total range."""
    range_sec = total_range_ms / 1000
    if range_sec <= 30: return 1000  # 1s
    if range_sec <= 90: return 2000  # 2s
    if range_sec <= 300: return 10000 # 10s
    return 30000 # 30s


def match_label_filters(entry, label_filters):
    """Return True if entry's labels match all label_filters (exact key=value)."""
    if not label_filters:
        return True
    labels = entry.get("labels") or {}
    return all(labels.get(k) == v for k, v in label_filters.items())


# In-process cache: avoids re-reading the same msgpack file multiple times per run.
# Key: cache_path → value: (source_mtime_threshold, data_dict)
_mem_cache = {}

def _metrics_cache_path(filepath, label_filters=None, latency_key=None):
    """Return the cache file path for a given combination of filter dimensions.

    All active dimensions — label filters, latency key, value range (-b), and
    time range (-t) — are folded into one hash.  The base (unfiltered) case for
    generic metrics (label_filters only, no bin/time/latency_key) uses a simple
    single-component hash for backward compatibility with existing cache files.

    File naming:
      - Labels or latency_key: <base>_<hash8>.kbcache.*
    """
    base = os.path.splitext(filepath)[0]
    ext = ".kbcache.msgpack" if _msgpack else ".kbcache.json"

    # Full hash: include every active dimension so each combination is unique.
    parts = {}
    if label_filters:
        parts['l'] = str(sorted(label_filters.items()))
    if latency_key:
        parts['k'] = latency_key
    key = hashlib.md5(str(sorted(parts.items())).encode()).hexdigest()[:8]
    return f"{base}_{key}{ext}"

def _load_cache(cache_path, source_mtime):
    """Return cached dict (with 'values' or 'stats' keys) or None.
    Checks _mem_cache first to avoid re-reading the file within the same run.
    Tries cache_path first; if msgpack path not found, falls back to the
    equivalent .kbcache.json for transparent upgrade of old caches."""
    logging.info(f"_load_cache : {cache_path}")
    # In-memory hit: same file, still valid for this source_mtime.
    mem = _mem_cache.get(cache_path)
    if mem is not None and mem[0] >= source_mtime:
        return mem[1]

    def _try(path):
        try:
            if not os.path.exists(path):
                logging.debug(f"does not exist {path}")
                return None
            if os.path.getmtime(path) < source_mtime:
                logging.debug(f"mtime < source_mtime {path}")
                return None
            fname = os.path.basename(path)
            if path.endswith('.msgpack') and _msgpack:
                logging.debug(f"opening {path}")
                with open(path, 'rb') as f:
                    with _spinner(desc=f"  Loading {fname}, please wait", show_timer=False):
                        data = _msgpack.unpackb(f.read(), raw=False)
            else:
                with open(path) as f:
                    with _spinner(desc=f"  Loading {fname}, please wait", show_timer=False):
                        data = json.load(f)
            if isinstance(data, list):      # upgrade old list format
                return {"values": data}
            return data if isinstance(data, dict) else None
        except Exception as e:
            logging.error(f"error loading cache: {e}")
            return None

    result = _try(cache_path)
    if result is None and cache_path.endswith('.msgpack'):
        logging.debug(f"cache miss, falling back to json {cache_path}")
        result = _try(cache_path[:-len('.msgpack')] + '.json')
    if result is not None:
        _mem_cache[cache_path] = (source_mtime, result)
    return result

def _save_cache(cache_path, data):
    logging.debug("_save_cache")
    try:
        fname = os.path.basename(cache_path)
        if cache_path.endswith('.msgpack') and _msgpack:
            with open(cache_path, 'wb') as f:
                with _spinner(desc=f"  Saving {fname}, please wait", show_timer=False):
                    f.write(_msgpack.packb(data, use_bin_type=True))
        else:
            with open(cache_path, 'w') as f:
                with _spinner(desc=f"  Saving {fname}, please wait", show_timer=False):
                    json.dump(data, f)
        # Keep in-memory cache in sync so subsequent reads in this run see the update.
        _mem_cache[cache_path] = (os.path.getmtime(cache_path), data)
    except Exception as e:
        logging.error(f"error saving cache: {e}")


def _get_cached_stats(cache_path, source_mtime):
    """Return pre-computed stats dict from cache if present, else None."""
    logging.debug("_get_cached_stats")
    cached = _load_cache(cache_path, source_mtime)
    return cached.get("stats") if cached else None

def _save_stats_to_cache(cache_path, source_mtime, values_key, values, stats_dict):
    """Persist stats into the cache file."""
    logging.debug("_save_stats_to_cache")
    try:
        cache_obj = _load_cache(cache_path, source_mtime) or {values_key: values}
        cache_obj["stats"] = stats_dict
        _save_cache(cache_path, cache_obj)
    except Exception as e:
        logging.error(f"error saving stats to cache: {e}")



def load_generic_metrics(filepath, label_filters=None, return_entries=False, need_labels=False):
    """
    Load a JSON list of metric objects (e.g. from collected-metrics), extract numeric
    'value' for each entry. If label_filters is a dict (e.g. {"id": "/kubepods.slice"}),
    only include entries whose labels match.

    Returns list of floats by default. When return_entries=True, returns minimal entry
    dicts {"timestamp": ..., "value": ...} served from cache when timestamps are cached,
    or full entry dicts (with labels) on a cold/upgraded parse.

    need_labels=True forces a full parse even if timestamps are cached, so that the
    returned entries carry their original label dicts (required for --source/-S).
    """
    logging.debug("load_generic_metrics")
    cache_path = _metrics_cache_path(filepath, label_filters)
    source_mtime = os.path.getmtime(filepath)

    _t0 = time.perf_counter()
    cached = _load_cache(cache_path, source_mtime)
    if cached is not None:
        logging.debug("cache is not None")
        _elapsed = time.perf_counter() - _t0
        values = cached.get("values", [])
        if not return_entries:
            logging.info(f"not return_entries. returning {len(values)} values")
            return values
        t0_raw = cached.get("t0_raw")
        if not need_labels:
            logging.debug(f"does not need_labels")
            timestamps = cached.get("timestamps")
            logging.debug(f"timestamps: {len(timestamps)} , len(values): {len(values)}")
            if timestamps is not None and len(timestamps) == len(values) and t0_raw is not None:
                # One-time migration: convert legacy string timestamps to epoch floats.
                # TODO delete ? We can just delete the cache files.
                if timestamps and isinstance(timestamps[0], str):
                    _debug(f"  (migrating {len(timestamps):,} string timestamps to epoch floats)")
                    timestamps = [
                        (dt.timestamp() if (dt := _parse_timestamp(ts)) is not None else 0.0)
                        for ts in timestamps
                    ]
                    _save_cache(cache_path, {**cached, "timestamps": timestamps})
                    logging.debug(f"cache saved")
                _t0 = time.perf_counter()
                tuple = [{"timestamp": ts, "elapsedTime": ts - t0_raw, "value": v}
                         for ts, v in zip(timestamps, values)]
                _elapsed = time.perf_counter() - _t0
                logging.debug(f"tuple zipped from cache in {_elapsed:.1f}s")
                return tuple
        else:
            logging.debug(f"needs_labels")
            # TODO Not clear to me why this is needed purely for a `-S`, when cardinality should already be cached.
            # need_labels=True: serve full entries from cache when labels are stored.
            # The base (unfiltered) cache always stores labels; label-filtered caches do not,
            # but those are handled by the warm path below (line ~372).
            labels = cached.get("labels")
            timestamps = cached.get("timestamps")
            if (labels is not None and timestamps is not None and t0_raw is not None
                    and len(labels) == len(values) and len(timestamps) == len(values)):
                _t0 = time.perf_counter()
                triples = [{"timestamp": ts, "elapsedTime": ts - t0_raw, "value": v, "labels": lab}
                           for ts, v, lab in zip(timestamps, values, labels)]
                _elapsed = time.perf_counter() - _t0
                logging.debug(f"values zipped from cache in {_elapsed:.1f}s")
                return triples
        # Old cache without timestamps/labels/t0_raw — fall through to full parse

    logging.debug("cached is None or needs_labels. Falling back to full kbcache")
    # Warm path for label-filtered calls: if the base (unfiltered) kbcache has labels,
    # filter in memory rather than re-reading the source file.
    if label_filters:
        logging.debug("has label_filters")
        base_path   = _metrics_cache_path(filepath)
        base_cached = _load_cache(base_path, source_mtime)
        if base_cached is not None and "labels" in base_cached and "t0_raw" in base_cached:
            _elapsed      = time.perf_counter() - _t0
            bc_t0_raw     = base_cached["t0_raw"]
            bc_values     = base_cached["values"]
            bc_timestamps = base_cached["timestamps"]
            bc_labels     = base_cached["labels"]
            triples = [(v, ts, lab)
                       for v, ts, lab in zip(bc_values, bc_timestamps, bc_labels)
                       if match_label_filters({"labels": lab}, label_filters)]
            logging.debug("triples filtered by labels")
            values     = [t[0] for t in triples]
            timestamps = [t[1] for t in triples]
            labs       = [t[2] for t in triples]
            key_counters = defaultdict(Counter)
            for lab in labs:
                for k, v in lab.items():
                    key_counters[k][v] += 1
            logging.debug("keys counted")
            _save_cache(cache_path, {
                "t0_raw": bc_t0_raw,
                "values": values,
                "timestamps": timestamps,
                "cardinality": {"None-None-None-None":
                                {k: dict(v) for k, v in key_counters.items()}},
            })
            logging.info(f"filtered from base cache in {_elapsed:.1f}s")
            if not return_entries:
                return values
            if need_labels:
                _t0 = time.perf_counter()
                zipped = [{"labels": lab, "elapsedTime": ts - bc_t0_raw, "value": v, "timestamp": ts}
                          for lab, v, ts in zip(labs, values, timestamps)]
                _elapsed = time.perf_counter() - _t0
                logging.debug(f"label-value-time tuple zipped from cache in {_elapsed:.1f}s")
                return zipped
            _t0 = time.perf_counter()
            zipped = [{"elapsedTime": ts - bc_t0_raw, "value": v, "timestamp": ts}
                      for v, ts in zip(values, timestamps)]
            _elapsed = time.perf_counter() - _t0
            logging.debug(f"value-time tuple zipped from cache in {_elapsed:.1f}s")
            return zipped

    logging.debug("no label_filters, no cache hits, loading raw json 🐌")
    # TODO try to get the full cache instead of the raw json, and parse from there.

    _t0 = time.perf_counter()
    entries      = []
    values       = []
    timestamps   = []
    labels_list  = []
    key_counters = defaultdict(Counter)
    fname = os.path.basename(filepath)
    with open(filepath, "rb") as f:
        _iter = _ijson.items(f, 'item') if _ijson else _json_load(f)
        if not _ijson and not isinstance(_iter, list):
            _iter = [_iter]
        for entry in _progress(_iter, desc=f"  Parsing {fname}"):
            if "value" not in entry:
                continue
            if not match_label_filters(entry, label_filters):
                continue
            try:
                fval = float(entry["value"])
            except (TypeError, ValueError):
                continue
            values.append(fval)
            ts_dt = _parse_timestamp(entry.get("timestamp", ""))
            ts_epoch = ts_dt.timestamp() if ts_dt is not None else 0.0
            timestamps.append(ts_epoch)
            lab = entry.get("labels") or {}
            labels_list.append(lab)
            for k, v in lab.items():
                key_counters[k][v] += 1
            if return_entries and need_labels:
                entries.append({**entry, "value": fval, "timestamp": ts_epoch})
    _elapsed = time.perf_counter() - _t0
    logging.info(f"  (raw loaded in {_elapsed:.1f}s)")
    # Sort by value before caching so callers can skip sorting on cache hits.
    # All entry consumers (scatter, clip_entries_to_time_range) re-sort by time themselves.
    paired = sorted(zip(values, timestamps, labels_list), key=lambda p: (p[0], p[1]))
    logging.debug(f"raw sorted and zipped in {_elapsed:.1f}s")
    values      = [p[0] for p in paired]
    timestamps  = [p[1] for p in paired]
    labels_list = [p[2] for p in paired]
    t0_raw = min(timestamps) if timestamps else 0.0
    cardinality_cache = {"None-None-None-None": {k: dict(v) for k, v in key_counters.items()}}
    if not label_filters:
        # Base cache: include labels to enable in-memory filtering for future --label calls.
        _save_cache(cache_path, {"t0_raw": t0_raw, "values": values, "timestamps": timestamps,
                                 "labels": labels_list, "cardinality": cardinality_cache})
    else:
        _save_cache(cache_path, {"t0_raw": t0_raw, "values": values, "timestamps": timestamps,
                                 "cardinality": cardinality_cache})
    if return_entries and need_labels:
        # elapsedTime was not known during the loop; stamp it now.
        for e in entries:
            e["elapsedTime"] = e["timestamp"] - t0_raw
    elif return_entries:
        # Reconstruct minimal entries from already-collected parallel arrays —
        # avoids building 5M full entry dicts during the parse loop.
        entries = [{"timestamp": ts, "elapsedTime": ts - t0_raw, "value": v}
                   for ts, v in zip(timestamps, values)]
    return entries if return_entries else values



def _load_lat_metrics_normalized(lat_path, latency_key, field_filters=None,
                                   min_val=None, max_val=None,
                                   tmin_sec=None, tmax_sec=None):
    """Load podLatencyMeasurement JSON in unified metrics format.

    Each source entry becomes
        {"timestamp": epoch_float, "elapsedTime": float, "value": float, "labels": dict}
    where elapsedTime is seconds from the fragment's global minimum timestamp (t0_raw),
    computed once at first parse and persisted in cache. All time-range filtering uses
    elapsedTime directly — no re-anchoring, no shift on subsequent filter passes.

    Base cache (all unfiltered entries for this latency_key):
        {"t0_raw": float, "values": [float,...], "timestamps": [float,...],
         "labels": [dict,...], "cardinality": {"None-None-None-None": {field: {val: count}}}}
    Old-format caches (without "t0_raw") are treated as misses and rebuilt.

    Subset cache for filtered combinations (field_filters + bin + time):
        via _metrics_cache_path() — same pattern as generic metrics.
    """
    logging.debug(f"_load_lat_metrics_normalized min,max {min_val},{max_val}")
    cache_path   = _metrics_cache_path(lat_path, latency_key=latency_key)
    source_mtime = os.path.getmtime(lat_path)
    _t0 = time.perf_counter()

    # Convert field_filters to label-filter format: {k: str(v)}
    effective_lf = {k: str(v) for k, v in field_filters.items()} if field_filters else None

    has_filters = (effective_lf or min_val is not None or max_val is not None
                   or tmin_sec is not None or tmax_sec is not None)
    subset_path = (_metrics_cache_path(lat_path, effective_lf, latency_key=latency_key)
                   if has_filters else None)

    # --- Subset cache fast path ---
    if subset_path and not (tmin_sec is not None or tmax_sec is not None):
        _subset = _load_cache(subset_path, source_mtime)
        if (_subset and "values" in _subset and "timestamps" in _subset
                and "labels" in _subset and "t0_raw" in _subset):
            _info(f"  (latency subset cache loaded in {time.perf_counter()-_t0:.1f}s)")
            _t0_raw = _subset["t0_raw"]
            return [{"timestamp": ts, "elapsedTime": ts - _t0_raw, "value": v, "labels": lab}
                    for v, ts, lab in zip(_subset["values"], _subset["timestamps"], _subset["labels"])]

    # --- Base cache warm path ---
    base_cached = _load_cache(cache_path, source_mtime)
    if base_cached is not None and "values" in base_cached and "t0_raw" in base_cached:
        _info(f"  (latency cache loaded in {time.perf_counter()-_t0:.1f}s)")
        t0_raw     = base_cached["t0_raw"]
        values     = base_cached["values"]
        timestamps = base_cached["timestamps"]
        labels     = base_cached["labels"]
        # Filter in-memory using the same match_label_filters() used by metrics path.
        if effective_lf:
            triples = [(v, ts, lab)
                       for v, ts, lab in zip(values, timestamps, labels)
                       if match_label_filters({"labels": lab}, effective_lf)]
            values     = [t[0] for t in triples]
            timestamps = [t[1] for t in triples]
            labels     = [t[2] for t in triples]
        entries = [{"timestamp": ts, "elapsedTime": ts - t0_raw, "value": v, "labels": lab}
                   for v, ts, lab in zip(values, timestamps, labels)]
    else:
        # --- Cold parse ---
        values_raw, timestamps_raw, labels_raw = [], [], []
        card = defaultdict(Counter)
        with open(lat_path, 'rb') as f:
            _iter = _ijson.items(f, 'item') if _ijson else _json_load(f)
            if not _ijson and not isinstance(_iter, list):
                _iter = []
            for i in _progress(_iter, desc="  Parsing latency"):
                if latency_key not in i or "timestamp" not in i:
                    continue
                raw_ts = i["timestamp"]
                epoch = raw_ts if isinstance(raw_ts, (int, float)) else (
                    lambda dt: dt.timestamp() if dt else None
                )(_parse_timestamp(raw_ts))
                if epoch is None:
                    continue
                lab = {k: str(v) for k, v in i.items()
                       if k not in _LAT_SKIP and not isinstance(v, dict)}
                values_raw.append(float(i[latency_key]))
                timestamps_raw.append(epoch)
                labels_raw.append(lab)
                for fk, fv in lab.items():
                    card[fk][fv] += 1
        # Sort by (value, timestamp) — same convention as load_generic_metrics.
        triplets = sorted(zip(values_raw, timestamps_raw, labels_raw), key=lambda p: (p[0], p[1]))
        sv = [p[0] for p in triplets]
        sts = [p[1] for p in triplets]
        sl = [p[2] for p in triplets]
        t0_raw = min(sts) if sts else 0.0
        _save_cache(cache_path, {
            "t0_raw":      t0_raw,
            "values":      sv,
            "timestamps":  sts,
            "labels":      sl,
            "cardinality": {"None-None-None-None": {k: dict(v) for k, v in card.items()}},
        })
        _info(f"  (latency cache built in {time.perf_counter()-_t0:.1f}s)")
        if effective_lf:
            triples = [(v, ts, lab)
                       for v, ts, lab in zip(sv, sts, sl)
                       if match_label_filters({"labels": lab}, effective_lf)]
            sv  = [t[0] for t in triples]
            sts = [t[1] for t in triples]
            sl  = [t[2] for t in triples]
        entries = [{"timestamp": ts, "elapsedTime": ts - t0_raw, "value": v, "labels": lab}
                   for v, ts, lab in zip(sv, sts, sl)]

    # Apply value range clip then time range filter via shared helpers.
    entries = clip_entries_to_range(entries, min_val, max_val)
    entries = clip_entries_to_time_range(entries, tmin_sec, tmax_sec)

    # Write subset cache (values + timestamps + labels) for filtered combos without time range,
    # so a subsequent warm run can skip the base cache entirely.
    if subset_path and entries and not (tmin_sec is not None or tmax_sec is not None):
        _save_cache(subset_path, {
            "t0_raw":     t0_raw,
            "values":     [e["value"]     for e in entries],
            "timestamps": [e["timestamp"] for e in entries],
            "labels":     [e["labels"]    for e in entries],
        })

    return entries


def parse_logfmt_line(line):
    pattern = r'(\w+)=(?:\"([^\"]*)\"|(\S+))'
    matches = re.findall(pattern, line)
    return {m[0]: (m[1] if m[1] else m[2]) for m in matches}

def extract_log_metrics(msg_content):
    metrics = {
        'p99': re.search(r"99th: (\d+)", msg_content),
        'max': re.search(r"max: (\d+)", msg_content),
        'avg': re.search(r"avg: (\d+)", msg_content)
    }
    return {k: (int(v.group(1)) if v else 0) for k, v in metrics.items()}

def find_pairs_recursively(fragments):
    pairs = []
    for root, dirs, files in os.walk('.'):
        for frag in fragments:
            metrics_dir_name = next((d for d in dirs if 'collected-metrics' in d and frag in d), None)
            log_match = next((f for f in files if frag in f and f.endswith(".log")), None)
            if metrics_dir_name or log_match:
                pairs.append({
                    'fragment': frag,
                    'metrics_dir': os.path.join(root, metrics_dir_name) if metrics_dir_name else None,
                    'log_path': os.path.join(root, log_match) if log_match else None,
                })
    return pairs

def compute_scheduling_throughput(metrics_list, latency_key=None):
    """
    Compute throughput from normalized podLatencyMeasurement entries.
    For each pod: event_time = timestamp (epoch) + value (ms) / 1000.
    Returns (max_pods_per_sec, avg_pods_per_sec, counts_per_second).

    metrics_list: list of {"timestamp": epoch_float, "value": float_ms, ...} dicts.
    latency_key: unused, kept for API compatibility.
    """
    second_counts = defaultdict(int)
    for e in metrics_list:
        ts_epoch = e.get("timestamp")
        val = e.get("value")
        if ts_epoch is None or val is None:
            continue
        try:
            event_epoch = float(ts_epoch) + float(val) / 1000.0
            second_counts[int(event_epoch)] += 1
        except (ValueError, TypeError):
            continue
    if not second_counts:
        return None, None, {}
    counts = list(second_counts.values())
    return max(counts), round(statistics.mean(counts), 2), dict(second_counts)



def analyze_label_cardinality(entries, top_n=10):
    """
    Given a list of raw metric entry dicts (each with a 'labels' key),
    return {label_key: Counter({label_value: count, ...}), ...}.
    Also prints a human-readable summary, capped at top_n values per label key.
    """
    logging.debug("analyze_label_cardinality")
    key_counters = defaultdict(Counter)
    for entry in entries:
        labels = entry.get("labels") or {}
        for k, v in labels.items():
            key_counters[k][v] += 1
    n = len(entries)
    print(f"  Labels across {n} entries:")
    for key in sorted(key_counters):
        counter = key_counters[key]
        total = len(counter)
        top = counter.most_common(top_n)
        parts = ", ".join(f"{v} ({c})" for v, c in top)
        suffix = f"  … +{total - top_n} more (see --top-labels)" if total > top_n else ""
        print(f"    {key:<12}: {parts}{suffix}")
    return dict(key_counters)


# Fields to skip when enumerating latency entry keys for cardinality analysis.
_LAT_SKIP = frozenset({"timestamp", "metricName"} | {t.value for t in LatencyType})



def _print_cardinality(card, top_n=10):
    """Print a cardinality dict (loaded from cache) in the same format."""
    print(f"  Fields (from cache):")
    for key in sorted(card):
        counter = card[key]  # plain dict {str_val: count} from JSON/msgpack
        total = len(counter)
        top = sorted(counter.items(), key=lambda x: -x[1])[:top_n]
        parts = ", ".join(f"{v} ({c})" for v, c in top)
        suffix = f"  … +{total - top_n} more" if total > top_n else ""
        print(f"    {key:<16}: {parts}{suffix}")


def _merge_cardinality_to_cache(cache_path, source_mtime, range_key, card):
    """Persist cardinality dict into cache['cardinality'][range_key]."""
    logging.debug("_merge_cardinality_to_cache")
    try:
        cached = _load_cache(cache_path, source_mtime) or {}
        if "cardinality" not in cached:
            cached["cardinality"] = {}
        # Convert Counter → plain dict for serialization
        cached["cardinality"][range_key] = {k: dict(v) for k, v in card.items()}
        _save_cache(cache_path, cached)
    except Exception as e:
        logging.error("error merging cardinality to cache {e}")


_SCATTER_MAX = 1_000_000  # random sample cap for scatter; full fidelity not needed at terminal res
_CDF_MAX     = 2_000   # monotone CDF needs ~2k points for smooth 70×12 rendering

# Color cycles for multi-fragment series (plotille terminal and plotly browser)
_SERIES_COLORS = ["cyan", "green", "magenta", "yellow", "red", "blue", "white"]
_PLOTLY_COLORS = ["#00bcd4", "#4caf50", "#e91e63", "#ff9800", "#f44336", "#2196f3", "#9c27b0"]
_PLOTLY_OUTPUT = "kube-burner-ocp-report.html"


def _plot_metrics_scatter(entries, title_suffix="", min_val=0):
    """Plot value over elapsed time for generic metric entries.

    Each entry must carry an 'elapsedTime' field (seconds from the fragment's
    global t0_raw, computed at parse time and persisted in cache).
    """
    n_orig = len(entries)
    if n_orig > _SCATTER_MAX:
        entries = random.sample(entries, _SCATTER_MAX)
    data_points = [(e["elapsedTime"], float(e["value"]))
                   for e in entries if "elapsedTime" in e and "value" in e]
    if not data_points:
        return
    data_points.sort(key=lambda x: x[0])
    x_secs = [p[0] for p in data_points]
    y_vals = [p[1] for p in data_points]
    title = f"[ Metrics Scatter (Time vs. Value){(' ' + title_suffix) if title_suffix else ''} ]"
    print(f"\n{title}")
    if n_orig > _SCATTER_MAX:
        _info(f"  (sampled {_SCATTER_MAX:,} of {n_orig:,} points)")
    fig = plotille.Figure()
    fig.set_x_limits(min_=min(x_secs), max_=max(x_secs))
    fig.set_y_limits(min_=min_val)
    fig.width, fig.height = 70, 12
    fig.scatter(x_secs, y_vals, lc='cyan')
    print(fig.show())


def _print_stats_table(title, n, avg, stdev, cv, p50, p90, p99,
                       min_val=None, max_val=None, unit="", extra_lines=None):
    """Print a standard stats summary block.

    min_val: shown when provided (metrics mode). Omitted for latency (run mode).
    unit: appended to numeric values, e.g. 'ms'.
    extra_lines: list of strings printed inside the box after the stat lines.
    """
    def fmt(v):
        return f"{v:.4g}" if isinstance(v, (int, float)) else str(v)
    u = unit
    print(f"\n\033[1;34m" + "=" * 50 + f" {title} " + "=" * 50 + "\033[0m")
    print(f"  N = {n}  |  avg = {fmt(avg)}{u}  |  stdev = {fmt(stdev)}  |  CV = {fmt(cv)}")
    parts = []
    if min_val is not None:
        parts.append(f"min = {fmt(min_val)}{u}")
    if max_val is not None:
        parts.append(f"max = {fmt(max_val)}{u}")
    parts += [f"P50 = {fmt(p50)}{u}", f"P90 = {fmt(p90)}{u}", f"P99 = {fmt(p99)}{u}"]
    print("  " + "  |  ".join(parts))
    for line in (extra_lines or []):
        print(f"  {line}")
    print("\033[1;34m" + "=" * 110 + "\033[0m")


def _plot_frequency_histogram(sorted_lats, cfg):
    """Plot snap-to-grid frequency histogram of scheduling latencies (clean ms boundaries)."""
    if cfg.no_hist:
        return
    lats_min, lats_max = min(sorted_lats), max(sorted_lats)
    step = get_pretty_step(lats_max - lats_min)
    snapped_data = [math.floor(x / step) * step for x in sorted_lats]
    counts = Counter(snapped_data)
    start_bucket = math.floor(lats_min / step) * step
    end_bucket = math.floor(lats_max / step) * step
    print(f"\n[ Frequency Histogram ({step/1000:g}s Buckets) ]")
    print(f"{'Bucket Range (ms)':<18} | {'Chart':<40} | Count")
    max_count = max(counts.values()) if counts else 1
    curr = start_bucket
    while curr <= end_bucket:
        cnt = counts.get(curr, 0)
        bar_len = int((cnt / max_count) * 40) if max_count > 0 else 0
        bar = "⣿" * bar_len
        print(f"[{curr:<7}, {curr+step:<7}) | {bar:<40} | {cnt}")
        curr += step


def _plot_histogram_plotille(sorted_vals, cfg, title_suffix="", bins=20):
    """Plot frequency histogram using plotille (for generic metric values)."""
    if cfg.no_hist or not sorted_vals:
        return
    title = f"[ Frequency Histogram {title_suffix} ]".strip()
    print(f"\n{title}")
    print(plotille.hist(sorted_vals, bins=bins))


def _plot_cdf(sorted_vals, cfg, title_suffix=""):
    """Plot cumulative distribution (CDF) for any sorted numeric values."""
    if cfg.no_cdf or not sorted_vals:
        return
    title = f"[ Cumulative Distribution (CDF) {title_suffix} ]".strip()
    print(f"\n{title}")
    n = len(sorted_vals)
    if n > _CDF_MAX:
        step = n // _CDF_MAX
        sorted_vals = sorted_vals[::step]
        n = len(sorted_vals)
    y_vals = [i / n for i in range(n)]
    fig = plotille.Figure()
    fig.set_x_limits(min_=min(sorted_vals))
    fig.set_y_limits(min_=0, max_=1)
    fig.width, fig.height = 70, 12
    fig.plot(sorted_vals, y_vals)
    print(fig.show())


def _plot_multi_scatter(series, title_suffix="", min_val=0):
    """Overlay multiple fragment scatter series on one plotille terminal figure.

    series: list of {"entries": [...], "label": str, "color": str}
    Each entry carries 'elapsedTime' (seconds from the fragment's global t0_raw),
    so all runs share a common 0-relative axis for easy comparison.
    """
    if not plotille or not series:
        return
    fig = plotille.Figure()
    fig.width, fig.height = 70, 12
    fig.set_y_limits(min_=min_val)
    all_x = []
    for s in series:
        entries = s["entries"]
        if not entries:
            continue
        if len(entries) > _SCATTER_MAX:
            entries = random.sample(entries, _SCATTER_MAX)
        x_secs = [e["elapsedTime"] for e in entries]
        y_vals = [e["value"] for e in entries]
        all_x.extend(x_secs)
        fig.scatter(x_secs, y_vals, lc=s["color"], label=s["label"])
    if not all_x:
        return
    fig.set_x_limits(min_=min(all_x), max_=max(all_x))
    title = f"[ Metrics Scatter — AGGREGATED{(' ' + title_suffix) if title_suffix else ''} ]"
    print(f"\n{title}")
    print(fig.show())


def _plot_plotly(series, agg=False, title_suffix="", output_html=None):
    """Generate interactive plotly figure with scatter / histogram / CDF subplots.

    series: list of {"entries": [...], "sorted_vals": [...], "label": str}
    agg: if True, all series data is merged into single traces;
         if False, each fragment gets its own named, toggle-able trace.
    Writes a self-contained HTML file and opens it in the default browser.
    """
    if not go:
        return
    out = output_html or _PLOTLY_OUTPUT
    fig = make_subplots(
        rows=3, cols=1,
        subplot_titles=[
            f"Scatter — {title_suffix}",
            f"Histogram — {title_suffix}",
            f"CDF — {title_suffix}",
        ],
        vertical_spacing=0.1,
    )
    colors = _PLOTLY_COLORS

    if agg:
        all_vals = sorted(v for s in series for v in s["sorted_vals"])
        all_entries = [e for s in series for e in (s.get("entries") or [])]
        if all_entries:
            x_secs = [e["elapsedTime"] for e in all_entries]
            y_vals = [e["value"] for e in all_entries]
            fig.add_trace(go.Scatter(x=x_secs, y=y_vals, mode="markers",
                                     name="All Fragments",
                                     marker=dict(color=colors[0], size=3)),
                          row=1, col=1)
        fig.add_trace(go.Histogram(x=all_vals, name="All Fragments",
                                   marker_color=colors[0], opacity=0.8),
                      row=2, col=1)
        n = len(all_vals)
        step = max(1, n // _CDF_MAX)
        fig.add_trace(go.Scatter(x=all_vals[::step],
                                  y=[i / n for i in range(0, n, step)],
                                  mode="lines", name="All Fragments", showlegend=False),
                      row=3, col=1)
    else:
        for i, s in enumerate(series):
            color = colors[i % len(colors)]
            label = s["label"]
            entries = s.get("entries") or []
            vals = s["sorted_vals"]
            if entries:
                x_secs = [e["elapsedTime"] for e in entries]
                y_vals = [e["value"] for e in entries]
                # TODO marginal_x/y are neat, but plotly express and cannot be used in subplots.
                # fig = px.scatter(x=x_secs, y=y_vals, marginal_x="histogram", marginal_y="rug", labels=label)
                fig.add_trace(go.Scatter(x=x_secs, y=y_vals, mode="markers",
                                          name=label, legendgroup=label,
                                          marker=dict(color=color, size=3)),
                              row=1, col=1)
            fig.add_trace(go.Histogram(x=vals, name=label, marker_color=color,
                                        opacity=0.6, legendgroup=label,
                                        showlegend=(not entries)),
                          row=2, col=1)
            n = len(vals)
            step = max(1, n // _CDF_MAX)
            fig.add_trace(go.Scatter(x=vals[::step],
                                      y=[i / n for i in range(0, n, step)],
                                      mode="lines", name=label, marker_color=color,
                                      legendgroup=label, showlegend=False),
                          row=3, col=1)

    fig.update_layout(
        barmode="overlay",
        title=f"kube-burner-ocp — {title_suffix}",
        height=900,
    )
    fig.write_html(out)
    print(f"  [plotly] Report written to {out}")
    fig.show()


def _render_plots_plotly(series, cfg, title_suffix):
    """Dispatch to _plot_plotly; exists so call sites read as a single verb."""
    _plot_plotly(series, agg=cfg.agg, title_suffix=title_suffix)


def _render_plots_plotille_agg(series, cfg, title_suffix="", min_val=None, max_val=None):
    """Render aggregated plotille scatter + histogram + CDF from a multi-fragment series list."""
    if not plotille or not series:
        return
    all_vals = sorted(v for s in series for v in s["sorted_vals"])
    use_pretty_hist = (min_val is None and max_val is None)
    _buf = io.StringIO()
    _buf.isatty = sys.stdout.isatty
    with _spinner(desc="  Building graphs"):
        with redirect_stdout(_buf):
            n = len(series)
            print(f"\n\033[1;34m{'='*25} VISUALS: AGGREGATED ({n} fragments) {'='*25}\033[0m")
            _plot_multi_scatter(series, title_suffix=title_suffix, min_val=min_val or 0)
            if use_pretty_hist:
                _plot_frequency_histogram(all_vals, cfg)
            else:
                _plot_histogram_plotille(all_vals, cfg, title_suffix)
            _plot_cdf(all_vals, cfg, title_suffix)
            print("\033[1;34m" + "="*70 + "\033[0m\n")
    print(_buf.getvalue(), end="")


def _render_plots_plotille_single(entries, plot_vals, cfg, title_suffix="",
                                   min_val=None, max_val=None, header=""):
    """Render single-fragment plotille scatter + histogram + CDF."""
    if not plotille:
        return
    use_pretty_hist = (min_val is None and max_val is None)
    _buf = io.StringIO()
    _buf.isatty = sys.stdout.isatty
    with _spinner(desc="  Building graphs"):
        with redirect_stdout(_buf):
            if header:
                print(f"\n\033[1;34m{'='*25} VISUALS: {header} {'='*25}\033[0m")
            _plot_metrics_scatter(entries, title_suffix=title_suffix, min_val=min_val or 0)
            if use_pretty_hist:
                _plot_frequency_histogram(plot_vals, cfg)
            else:
                _plot_histogram_plotille(plot_vals, cfg, title_suffix)
            _plot_cdf(plot_vals, cfg, title_suffix)
            if header:
                print("\033[1;34m" + "="*70 + "\033[0m\n")
    print(_buf.getvalue(), end="")


def _render_metrics_plotille_agg(series, cfg, display_base, min_val, max_val):
    """Aggregate plotille histogram + CDF (and optional scatter) for metrics mode."""
    if not plotille or not series:
        return
    all_vals = sorted(v for s in series
                      for v in clip_to_range(s["sorted_vals"], min_val, max_val))
    if not all_vals:
        return
    _buf = io.StringIO()
    _buf.isatty = sys.stdout.isatty
    with _spinner(desc="  Building graphs"):
        with redirect_stdout(_buf):
            if cfg.scatter:
                scatter_series = [s for s in series if s.get("entries")]
                if scatter_series:
                    _plot_multi_scatter(scatter_series, title_suffix=display_base)
            _plot_histogram_plotille(all_vals, cfg, f"AGGREGATED — {display_base}")
            _plot_cdf(all_vals, cfg, f"AGGREGATED — {display_base}")
    print(_buf.getvalue(), end="")


def clip_to_range(values, min_val=None, max_val=None):
    """Filter values to [min_val, max_val]. None means no bound."""
    if min_val is None and max_val is None:
        return values
    return [v for v in values
            if (min_val is None or v >= min_val)
            and (max_val is None or v <= max_val)]

def clip_entries_to_range(entries, min_val=None, max_val=None):
    """Filter entry dicts to those whose 'value' falls in [min_val, max_val]."""
    logging.debug("clip_entries_to_range")
    if min_val is None and max_val is None:
        return entries
    return [e for e in entries
            if (min_val is None or e["value"] >= min_val)
            and (max_val is None or e["value"] <= max_val)]


def clip_entries_to_time_range(entries, tmin_sec=None, tmax_sec=None):
    """Filter entry dicts to those whose elapsedTime falls within [tmin_sec, tmax_sec].
    elapsedTime is pre-computed relative to the fragment's global t0_raw at parse time."""
    logging.debug("clip_entries_to_time_range")
    if tmin_sec is None and tmax_sec is None:
        return entries
    filtered = [
        e for e in entries
        if (tmin_sec is None or e["elapsedTime"] >= tmin_sec)
        and (tmax_sec is None or e["elapsedTime"] <= tmax_sec)
    ]
    if not filtered:
        lo = f"{tmin_sec}s" if tmin_sec is not None else "start"
        hi = f"{tmax_sec}s" if tmax_sec is not None else "end"
        logging.warning(f"No entries in time window [{lo}, {hi}]")
    return filtered



def process_automation(uuid_fragments, cfg, min_val=None, max_val=None,
                       latency_key="podReadyLatency", field_filters=None,
                       tmin_sec=None, tmax_sec=None):
    if not uuid_fragments:
        print(f"Usage: kb-parse [--no-visuals] <fragment1> <fragment2> ...")
        return

    discovered_pairs = find_pairs_recursively(uuid_fragments)
    if not discovered_pairs:
        print(f"No collocated pairs found for fragments: {uuid_fragments}")
        return

    results = []
    fragment_series = []  # collected for --agg / --plotly post-loop rendering

    for pair in discovered_pairs:
        data = {'uuid_fragment': pair['fragment']}

        # Log Logic
        try:
            if not pair.get('log_path'):
                raise FileNotFoundError(f"no .log file found for fragment {pair['fragment']!r}")
            with open(pair['log_path'], 'r') as f:
                lines = f.readlines()
                if lines:
                    data['end time'] = parse_logfmt_line(lines[-1]).get('time', DEFAULT_VAL)
                    for line in lines:
                        parsed = parse_logfmt_line(line)
                        msg = parsed.get('msg', '')
                        if "Starting kube-burner" in msg:
                            # timestamp from Starting kube-burner should work. End time can stay as the last line, in the event it times out
                            data['start time'] = parsed.get('time', DEFAULT_VAL)
                            u_match = re.search(r"UUID ([a-f0-9\-]+)", msg)
                            v_match = re.search(r"\((.*?)\)", msg)
                            if u_match: data['UUID'] = u_match.group(1)
                            if v_match: data['k-b version'] = v_match.group(1).split('@')[0]
                        if "took" in msg and "Job" in msg:
                            d_match = re.search(r"took ([\w\.]+)", msg)
                            if d_match: data['job_took'] = d_match.group(1)
                        if "PodScheduled" in msg:
                            m = extract_log_metrics(msg)
                            data.update({'sched_p99': m['p99'], 'sched_max': m['max'], 'sched_avg': m['avg']})
                        if "ContainersReady" in msg:
                            m = extract_log_metrics(msg)
                            data.update({'ready_p99': m['p99'], 'ready_max': m['max'], 'ready_avg': m['avg']})
        except Exception as e:
            print(f"  [!] Log Error: {e}")

        # B. JSON Processing (only when a collected-metrics dir was found)
        if not pair.get('metrics_dir'):
            results.append({col: data.get(col, DEFAULT_VAL) for col in COLUMN_ORDER})
            continue
        summary_path = os.path.join(pair['metrics_dir'], SUMMARY_FILENAME)
        try:
            with open(summary_path, 'r') as f:
                summary_data = json.load(f)[0]
                data.update({
                    'OCP Version': summary_data.get('ocpVersion', DEFAULT_VAL),
                    'Note': summary_data.get('workloadMods', DEFAULT_VAL),
                    'scheduler': summary_data.get('scheduler', DEFAULT_VAL),
                    'podReplicas': summary_data.get('podReplicas', DEFAULT_VAL),
                    'workers': summary_data.get('otherNodesCount', 0),
                    'worker reserved cores': summary_data.get('workerReservedCores', DEFAULT_VAL),
                    'worker CPUs': summary_data.get('workerCPUs', DEFAULT_VAL),
                    'topologyPolicy': summary_data.get('topologyPolicy', DEFAULT_VAL)
                })
                job_cfg = summary_data.get('jobConfig', {})
                data['workload'] = job_cfg.get('name', DEFAULT_VAL)
                data['iterations'] = job_cfg.get('jobIterations', 0)
                data['qps burst'] = job_cfg.get('qps', DEFAULT_VAL)
                churn = job_cfg.get('churnConfig', {})
                data.update({
                    'cycles': churn.get('cycles', DEFAULT_VAL),
                    'percent': churn.get('percent', DEFAULT_VAL),
                    'duration': f"{int(churn.get('duration', 0) / 60_000_000_000)}m"
                })
        except Exception as e: print(f"  [!] Summary JSON Error: {e}")

        lat_path = os.path.join(pair['metrics_dir'], f"podLatencyMeasurement-{data['workload']}.json")

        try:
            entries = _load_lat_metrics_normalized(
                lat_path, latency_key,
                field_filters=field_filters,
                min_val=min_val, max_val=max_val,
                tmin_sec=tmin_sec, tmax_sec=tmax_sec,
            )
            if entries:
                # Clipping is already applied inside _load_lat_metrics_normalized.
                sorted_vals = [e["value"] for e in entries]
                n_vals = len(sorted_vals)

                plot_vals = sorted_vals

                # Collect for post-loop aggregate / plotly rendering (always).
                fragment_series.append({
                    "entries":     entries,
                    "sorted_vals": plot_vals,
                    "label":       pair["fragment"][:8],
                    "color":       _SERIES_COLORS[len(fragment_series) % len(_SERIES_COLORS)],
                })

                # Per-fragment plotille plots — skipped when --agg or --plotly is active.
                if not cfg.no_visuals and plotille and not cfg.agg and not cfg.plotly:
                    _t0 = time.perf_counter()
                    _render_plots_plotille_single(
                        entries, plot_vals, cfg,
                        title_suffix=latency_key,
                        min_val=min_val, max_val=max_val,
                        header=f"{data['scheduler']} {pair['fragment']}",
                    )
                    _elapsed = time.perf_counter() - _t0
                    if _elapsed > 0.5:
                        _info(f"  (built graphs in {_elapsed:.1f}s)")

                # Stats: try cache first (only for unfiltered, unconstrained runs).
                cache_path = _metrics_cache_path(lat_path, latency_key=latency_key)
                source_mtime = os.path.getmtime(lat_path)
                use_stats_cache = (not field_filters and min_val is None and max_val is None
                                   and tmin_sec is None and tmax_sec is None)
                cached_stats = _get_cached_stats(cache_path, source_mtime) if use_stats_cache else None
                if cached_stats:
                    data.update(cached_stats)
                    _info("  (stats cache loaded)")
                else:
                    _t0 = time.perf_counter()
                    _s = _compute_stats(sorted_vals)
                    data['stddev'] = round(_s["stdev"], 2)
                    data['avg']    = round(_s["avg"], 2)
                    data['max']    = _s["max"]
                    data['CV']     = _s["cv"]
                    data['sched_p90'] = round(_s["p90"], 2)
                    dist = statistics.quantiles(sorted_vals, n=20) if n_vals >= 20 else None
                    for idx, qh in enumerate(QUANTILES_HEADERS):
                        data[qh] = round(dist[idx], 2) if dist else round(_s["p50"], 2)
                    max_pps, avg_pps, _ = compute_scheduling_throughput(entries)
                    data['max_pods_per_sec'] = max_pps if max_pps is not None else DEFAULT_VAL
                    data['avg_pods_per_sec'] = avg_pps if avg_pps is not None else DEFAULT_VAL
                    _elapsed = time.perf_counter() - _t0
                    if _elapsed > 0.5:
                        _info(f"  (crunched numbers in {_elapsed:.1f}s)")
                    if use_stats_cache:
                        stats_to_cache = {k: data[k]
                                          for k in (['stddev', 'avg', 'max', 'CV', 'sched_p90',
                                                      'max_pods_per_sec', 'avg_pods_per_sec']
                                                     + QUANTILES_HEADERS)
                                          if k in data}
                        _save_stats_to_cache(cache_path, source_mtime, "values",
                                             sorted_vals, stats_to_cache)

                _print_stats_table(
                    f"LATENCY STATS: {pair['fragment']}", n_vals,
                    data.get('avg', '-'), data.get('stddev', '-'), data.get('CV', '-'),
                    data.get('P50', '-'), data.get('P90', '-'), data.get('p99', '-'),
                    max_val=data.get('max', '-'), unit="ms",
                )

            # Not the cleanest, but cached stats are separated by latency type, so we
            # simply append the column for CSV prior to printing time
            if latency_key == LatencyType.schedulingLatency.value:
                logging.debug(f"{data}")
                data['sched_stddev'] = data['stddev'] if data['stddev'] is not None else DEFAULT_VAL

            if cfg.source:
                range_key = f"{min_val}-{max_val}-{tmin_sec}-{tmax_sec}"
                cache_path_s = _metrics_cache_path(lat_path, latency_key=latency_key)
                source_mtime_s = os.path.getmtime(lat_path)
                cached_card = (_load_cache(cache_path_s, source_mtime_s) or {}).get(
                    "cardinality", {}).get(range_key)
                if cached_card:
                    _info("  (cardinality cache loaded)")
                    _print_cardinality(cached_card, top_n=cfg.top_labels)
                elif entries is not None:
                    # entries already have "labels" — no second parse needed.
                    card = analyze_label_cardinality(
                        clip_entries_to_range(entries, min_val, max_val), top_n=cfg.top_labels)
                    _merge_cardinality_to_cache(cache_path_s, source_mtime_s, range_key, card)
        except Exception as e: logging.error(f"  [!] Metrics JSON Error: {e}")


        results.append({col: data.get(col, DEFAULT_VAL) for col in COLUMN_ORDER})

    # Post-loop: aggregate or plotly rendering (--agg / --plotly)
    if not cfg.no_visuals and fragment_series:
        if cfg.plotly:
            _render_plots_plotly(fragment_series, cfg, title_suffix=latency_key)
        elif cfg.agg:
            _render_plots_plotille_agg(
                fragment_series, cfg, title_suffix=latency_key,
                min_val=min_val, max_val=max_val,
            )
        # else: already rendered per-fragment inside the loop above

    # Summary Table — always printed
    print("\n" + " " * 20 + "\033[1;32m📊 FINAL COMPARISON SUMMARY\033[0m")
    print(f"{'Fragment':<12} | {'Scheduler':<15} | {'Replicas':<10} | {'Avg (ms)':<10} | {'Max pods/s':<10} | {'Avg pods/s':<10} | {'Consistency (CV)':<15}")
    print("-" * 110)
    for r in results:
        cv = r.get('CV', 0)
        status = _classify_cv(cv)
        print(f"{str(r.get('UUID',''))[:8]:<12} | {r.get('scheduler',''):<15} | {r.get('podReplicas',''):<10} | {r.get('avg',''):<10} | {r.get('max_pods_per_sec',''):<10} | {r.get('avg_pods_per_sec',''):<10} | {cv:<15} {status}")

    # CSV Dump (always write to file)
    with open(OUTPUT_FILE, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=COLUMN_ORDER, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)

    # Write to Standard Out (Console) — always printed
    print("\n" + "="*30 + " CSV RAW DATA " + "="*30)
    console_writer = csv.DictWriter(sys.stdout, fieldnames=COLUMN_ORDER, extrasaction='ignore')
    console_writer.writeheader()
    console_writer.writerows(results)


def _compute_stats(sorted_vals):
    """Return summary statistics for a pre-sorted list of numeric values.

    Returns a dict with keys: n, avg, stdev, cv, p50, p90, p99, min, max.
    Returns all-zero dict when sorted_vals is empty.
    """
    n = len(sorted_vals)
    if n == 0:
        return {"n": 0, "avg": 0.0, "stdev": 0.0, "cv": 0.0,
                "p50": 0.0, "p90": 0.0, "p99": 0.0, "min": 0.0, "max": 0.0}
    avg   = statistics.mean(sorted_vals)
    stdev = statistics.stdev(sorted_vals) if n > 1 else 0.0
    cv    = round(stdev / avg, 3) if avg else 0.0
    p50   = statistics.median(sorted_vals)
    p90   = statistics.quantiles(sorted_vals, n=10)[8] if n >= 10 else sorted_vals[-1]
    p99   = statistics.quantiles(sorted_vals, n=100)[98] if n >= 100 else sorted_vals[-1]
    return {"n": n, "avg": avg, "stdev": stdev, "cv": cv,
            "p50": p50, "p90": p90, "p99": p99,
            "min": sorted_vals[0], "max": sorted_vals[-1]}


def run_generic_metrics_analysis(filepath, cfg, metric_name=None, label_filters=None,
                                  min_val=None, max_val=None,
                                  tmin_sec=None, tmax_sec=None, _collect=False):
    """
    Run histogram, CDF, and CV (and summary stats) on a metrics JSON file that has
    objects with 'value' and optional 'labels'. Use label_filters to restrict
    (e.g. {"id": "/kubepods.slice", "node": "e26-h21-000-r650"}).

    When _collect=True: suppresses all visual output and returns
    (sorted_vals, entries) for the caller to aggregate across fragments.
    Stats tables and cardinality are still printed. Caches are read/written normally.
    """
    if not os.path.isfile(filepath):
        print(f"[!] Not a file: {filepath}", file=sys.stderr)
        return
    logging.info("run_generic_metrics_analysis")
    # Raw entries (with timestamps) are needed for scatter (--scatter), label source
    # lookup (--source), or time filtering.  Histogram and CDF run from cached values
    # alone, so the default path hits the cache without touching the source file.
    cache_path = _metrics_cache_path(filepath, label_filters)
    source_mtime = os.path.getmtime(filepath)
    logging.info(f"_metrics_cache_path: {cache_path}")

    # Pre-check cardinality cache so we avoid forcing a cold source parse just for
    # -S when the result is already stored.  need_labels stays False on a warm hit.
    range_key = f"{min_val}-{max_val}-{tmin_sec}-{tmax_sec}"
    cached_card = None
    if cfg.source:
        cached_card = (_load_cache(cache_path, source_mtime) or {}).get("cardinality", {}).get(range_key)

    need_labels = cfg.source and cached_card is None
    need_entries = cfg.scatter or need_labels or (tmin_sec is not None or tmax_sec is not None)
    logging.debug(f"need_entries={need_entries}")

    entries = None
    if need_entries:
        logging.info("not _subset_fast_path and does need_entries")
        entries = load_generic_metrics(filepath, label_filters=label_filters,
                                        return_entries=True, need_labels=need_labels)
        if tmin_sec is not None or tmax_sec is not None:
            original_n = len(entries)
            entries = clip_entries_to_time_range(entries, tmin_sec, tmax_sec)
            if not entries:
                return
            logging.info(f"time window [{tmin_sec}s, {tmax_sec}s]: {len(entries)}/{original_n} entries")
        _t0 = time.perf_counter()
        values = [e["value"] for e in entries]
        logging.debug(f"  (extracted {len(values):,} values in {time.perf_counter()-_t0:.2f}s)")
        # Pre-sorted when entries came from cache in value order (no time-filter, no
        # cold label parse). Time-filter picks a non-contiguous subset; a cold
        # need_labels parse returns file order — neither is guaranteed sorted.
        values_presorted = not need_labels and (tmin_sec is None and tmax_sec is None)
    else:
        logging.info("not _subset_fast_path and does not need_entries")
        values = load_generic_metrics(filepath, label_filters=label_filters)
        values_presorted = True   # load_generic_metrics caches sorted values

    if not values:
        print(f"[!] No values found in {filepath}" + (
            f" with label filters {label_filters}" if label_filters else ""
        ), file=sys.stderr)
        return

    # TODO delete metric_name as is unused
    display_name = metric_name or os.path.basename(filepath)
    title_suffix = f" — {display_name}" if display_name else ""
    if label_filters:
        title_suffix += " " + str(label_filters)

    if values_presorted:
        sorted_vals = values
        logging.debug(f"  (skipped sort — {len(sorted_vals):,} values pre-sorted from cache)")
    else:
        _t0 = time.perf_counter()
        sorted_vals = sorted(values)
        logging.debug(f"  (sorted {len(sorted_vals):,} values in {time.perf_counter()-_t0:.2f}s)")
    n = len(sorted_vals)

    _t0 = time.perf_counter()
    cached_stats = _get_cached_stats(cache_path, source_mtime)
    logging.debug(f"  (stats cache lookup in {time.perf_counter()-_t0:.2f}s)")
    if cached_stats:
        avg   = cached_stats["avg"]
        stdev = cached_stats["stdev"]
        cv    = cached_stats["cv"]
        p50   = cached_stats["p50"]
        p90   = cached_stats["p90"]
        p99   = cached_stats["p99"]
    else:
        _t0 = time.perf_counter()
        with _spinner(desc="  Computing stats", show_timer=True):
            _s = _compute_stats(sorted_vals)
        _info(f"  (crunched numbers in {time.perf_counter()-_t0:.1f}s)")
        avg, stdev, cv, p50, p90, p99 = (_s["avg"], _s["stdev"], _s["cv"],
                                            _s["p50"], _s["p90"], _s["p99"])
        _t0 = time.perf_counter()
        _save_stats_to_cache(cache_path, source_mtime, "values", values,
                                {"avg": avg, "stdev": stdev, "cv": cv,
                                "p50": p50, "p90": p90, "p99": p99,
                                "min": _s["min"], "max": _s["max"]})
        logging.debug(f"  (stats cache written in {time.perf_counter()-_t0:.2f}s)")

    # If a value range filter is active, recompute display stats on the visible subset.
    # Cache always stores full-dataset stats; these are display-only overrides.
    if min_val is not None or max_val is not None:
        display_vals = clip_to_range(sorted_vals, min_val, max_val)
        if display_vals:
            dn    = len(display_vals)
            n     = dn
            avg   = statistics.mean(display_vals)
            stdev = statistics.stdev(display_vals) if dn > 1 else 0.0
            cv    = round(stdev / avg, 3) if avg else 0.0
            p50   = statistics.median(display_vals)
            p90   = statistics.quantiles(display_vals, n=10)[8] if dn >= 10 else display_vals[-1]
            p99   = statistics.quantiles(display_vals, n=100)[98] if dn >= 100 else display_vals[-1]
    else:
        display_vals = sorted_vals

    extra = [f"Label filters: {label_filters}"] if label_filters else None
    _print_stats_table(
        f"METRICS: {display_name}", n, avg, stdev, cv, p50, p90, p99,
        min_val=min(display_vals), max_val=max(display_vals), extra_lines=extra,
    )

    # Show label cardinality only when --source is explicitly requested.
    # range_key and cached_card were computed before the load_generic_metrics call
    # so a warm cache hit avoids re-reading the source file entirely.
    if cfg.source:
        if cached_card:
            logging.info("  (cardinality cache loaded)")
            _print_cardinality(cached_card, top_n=cfg.top_labels)
        elif entries is not None:
            card = analyze_label_cardinality(clip_entries_to_range(entries, min_val, max_val), top_n=cfg.top_labels)
            _merge_cardinality_to_cache(cache_path, source_mtime, range_key, card)

    if _collect:
        # Caller handles rendering; return data for aggregation.
        logging.info("  (analysis complete — data returned for aggregation)")
        return sorted_vals, entries

    if not cfg.no_visuals and plotille:
        # Fast path: sorted_vals is already the filtered subset — no clip needed.
        # Slow path: clip the full sorted_vals to the display range.
        plot_vals = clip_to_range(sorted_vals, min_val, max_val)
        if not plot_vals:
            print(f"  [!] No values in range [{min_val}, {max_val}]")
        else:
            if len(plot_vals) < len(sorted_vals):
                _info(f"  (showing {len(plot_vals)}/{len(sorted_vals)} values in [{min_val}, {max_val}])")
            _t0 = time.perf_counter()
            _buf = io.StringIO()
            _buf.isatty = sys.stdout.isatty  # preserve TTY status so plotille emits ANSI colours
            with _spinner(desc="  Building graphs"):
                with redirect_stdout(_buf):
                    if cfg.scatter and entries is not None:
                        _plot_metrics_scatter(clip_entries_to_range(entries, min_val, max_val), title_suffix=title_suffix, min_val=min_val)
                    _plot_histogram_plotille(plot_vals, cfg, title_suffix=title_suffix)
                    _plot_cdf(plot_vals, cfg, title_suffix=title_suffix)
            print(_buf.getvalue(), end="")
            _elapsed = time.perf_counter() - _t0
            if _elapsed > 0.5:
                logging.info(f"  (built graphs in {_elapsed:.1f}s)")
    elif cfg.no_visuals:
        logging.info("  (Use without --no-visuals to see histogram and CDF; add --scatter for scatter plot.)")
    logging.info("  (analysis complete)")


def _classify_cv(cv):
    """Return a human-readable scheduling pattern label for a CV value."""
    if not isinstance(cv, (int, float)):
        return "—"
    if cv >= 0.8:
        return "🔥 Bursty (optimal)"
    if cv >= 0.7:
        return "⚠️ Likely serial - check scatterplot"
    return "🐌 Serial scheduling rate"


def _parse_bucket_arg(raw_str, flag_name):
    """Parse 'MIN, MAX', ',MAX', or 'MIN' into (lo, hi). Exits on error."""
    logging.debug(f"_parse_bucket_arg {flag_name}={raw_str}")
    raw = raw_str.strip().strip('[]()')
    try:
        if ',' in raw:
            left, right = raw.split(',', 1)
            return (float(left.strip()) if left.strip() else None,
                    float(right.strip()) if right.strip() else None)
        return float(raw), None
    except ValueError:
        print(f"[!] --{flag_name}: expected 'MIN, MAX', ',MAX', or 'MIN', "
              f"got: {raw_str!r}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse kube-burner metrics and produce CSV reports.",
        epilog="%(prog)s UUID  — latency analysis.  "
               "%(prog)s UUID metrics FILE ...  — metrics analysis (order of args is irrelevant). "
               "Examples: '%(prog)s UUID' or '%(prog)s UUID metrics containerCPU cgroupCPU'",
    )
    parser.add_argument("--no-visuals", action="store_true",
                        help="Disable the large terminal plots (scatterplot, histogram, CDF) to save space.")
    parser.add_argument("--metric-name", "-m", default=None, help="Display name for metric (metrics mode only; e.g. cgroupCPU).")
    parser.add_argument("--label", "-l", action="append", metavar="KEY=VALUE",
                        help="Filter by label in metrics mode (repeatable, e.g. -l id=/kubepods.slice).")
    parser.add_argument("--min", "-n", type=float, default=None, metavar="VALUE",
                        help="Only plot values >= VALUE (stats use full dataset).")
    parser.add_argument("--max", "-x", type=float, default=None, metavar="VALUE",
                        help="Only plot values <= VALUE (stats use full dataset).")
    parser.add_argument("--scatter", "-s", action="store_true",
                        help="In metrics mode: render a scatter plot of value vs. elapsed time. "
                             "Served from cache after the first run — fast for iterative drilldown.")
    parser.add_argument("--source", "-S", action="store_true",
                        help="In metrics mode: show label distributions for entries within the active "
                             "value range (--min/--max/--bucket). Always reads from source file on first run.")
    parser.add_argument("--top-labels", type=int, default=10, metavar="N",
                        help="Max label values to show per key in cardinality output (default: 10).")
    parser.add_argument("--bucket", "-b", default=None, metavar='"MIN, MAX"',
                        help='Plot range from histogram bucket label, e.g. --bucket "12083200, 12096000". '
                             'Overrides --min/--max if both are given.')
    parser.add_argument("--tbucket", "-t", default=None, metavar='"START_SEC, END_SEC"',
                        help='Filter metrics to a time window by elapsed seconds from the scatter X-axis, '
                             'e.g. --tbucket "30, 90". Analogous to --bucket for values.')
    parser.add_argument("--latency-type", "-L",
                        type=_resolve_latency_type,
                        default="podReadyLatency",
                        metavar="TYPE",
                        help=(
                            "Latency field to analyze from podLatencyMeasurement JSON "
                            "(unambiguous prefix accepted, e.g. -L s for schedulingLatency). "
                            "Choices: " + ", ".join(t.value for t in LatencyType) + ". "
                            "Default: podReadyLatency."
                        ))
    parser.add_argument("--agg", action="store_true",
                        help="Combine all fragments' data into one aggregated scatter/histogram/CDF "
                             "instead of separate plots per fragment.")
    parser.add_argument("--plotly", action="store_true",
                        help="Use plotly interactive HTML output (opens in browser) instead of "
                             "plotille terminal plots. Compatible with --agg.")
    parser.add_argument("--no-hist", action="store_true",
                        help="Suppress the frequency histogram plot.")
    parser.add_argument("--no-cdf", action="store_true",
                        help="Suppress the CDF plot.")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show all timing detail including intermediate steps.")
    parser.add_argument("--quiet", "-q", action="store_true",
                        help="Suppress all diagnostic output; show only stats tables and graphs.")
    parser.add_argument("positionals", nargs="*",
                        help="UUID fragment(s) plus optional 'metrics' keyword and metric file name(s) in any order "
                             "(e.g. '2178a534 metrics containerCPU' or 'metrics containerCPU 2178a534').")

    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
        _verbose = args.verbose
    elif args.quiet:
        logging.basicConfig(level=logging.WARN, format=LOG_FORMAT)
        _quiet   = args.quiet and not args.verbose
    else:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    if args.bucket is not None:
        args.min, args.max = _parse_bucket_arg(args.bucket, "bucket")
    if args.tbucket is not None:
        args.tmin, args.tmax = _parse_bucket_arg(args.tbucket, "tbucket")
    else:
        args.tmin = args.tmax = None

    cfg = RenderConfig(
        no_visuals=args.no_visuals,
        scatter=args.scatter,
        source=args.source,
        top_labels=args.top_labels,
        agg=args.agg,
        plotly=args.plotly,
        no_hist=args.no_hist,
        no_cdf=args.no_cdf,
    )

    positionals = args.positionals

    if not positionals:
        parser.print_help()
        sys.exit(1)

    # Classify positionals:
    #   'metrics'  → enables metrics mode
    #   everything else → candidates (UUID fragments or metric file names)
    # To tell UUIDs from metric files: run discovery on all candidates; anything that
    # matches a real .log or collected-metrics-* entry is a UUID fragment, the rest are
    # metric file names.  The caller guarantees no collision between the two sets.
    run_metrics = "metrics" in positionals
    candidates = [p for p in positionals if p != "metrics"]

    discovered_pairs = []
    if candidates:
        discovered_pairs = find_pairs_recursively(candidates)
    matched_fragments = {pair["fragment"] for pair in discovered_pairs}

    fragments    = [c for c in candidates if c in matched_fragments]
    metric_files = [c for c in candidates if c not in matched_fragments] if run_metrics else []

    if not fragments and not metric_files:
        parser.print_help()
        sys.exit(1)

    if run_metrics and not metric_files:
        print("[!] 'metrics' requires at least one metric file name after it.", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    # Parse -l KEY=VALUE pairs once; used by both latency and metrics modes.
    label_filters = {}
    if getattr(args, "label", None):
        for s in args.label:
            if "=" in s:
                k, v = s.split("=", 1)
                label_filters[k.strip()] = v.strip()

    if fragments and not run_metrics:
        process_automation(fragments, cfg, min_val=args.min, max_val=args.max,
                           latency_key=args.latency_type,
                           field_filters=label_filters or None,
                           tmin_sec=args.tmin, tmax_sec=args.tmax)

    if run_metrics and metric_files:
        if not discovered_pairs:
            print(f"[!] No collected-metrics dirs found — cannot locate metric files.", file=sys.stderr)
            sys.exit(1)
        use_agg_path = args.agg or args.plotly
        for raw_file in metric_files:
            metric_file = raw_file if raw_file.endswith(".json") else raw_file + ".json"
            # TODO remove metric_name
            display_base = getattr(args, "metric_name", None) or os.path.splitext(metric_file)[0]
            agg_series = []
            for pair in discovered_pairs:
                if not pair.get("metrics_dir"):
                    continue
                filepath = os.path.join(pair["metrics_dir"], metric_file)
                if not os.path.isfile(filepath):
                    print(f"[!] Not found: {filepath}", file=sys.stderr)
                    continue
                metric_name = display_base if len(discovered_pairs) == 1 else f"{display_base} ({pair['fragment']})"
                if use_agg_path:
                    result = run_generic_metrics_analysis(
                        filepath,
                        RenderConfig(no_visuals=True, scatter=cfg.scatter,
                                     source=cfg.source, top_labels=cfg.top_labels),
                        metric_name=metric_name,
                        label_filters=label_filters or None,
                        min_val=args.min,
                        max_val=args.max,
                        tmin_sec=args.tmin,
                        tmax_sec=args.tmax,
                        _collect=True,
                    )
                    if result:
                        sorted_vals, entries = result
                        agg_series.append({
                            "sorted_vals": sorted_vals,
                            "entries":     entries,
                            "label":       pair["fragment"][:8],
                            "color":       _SERIES_COLORS[len(agg_series) % len(_SERIES_COLORS)],
                        })
                else:
                    run_generic_metrics_analysis(
                        filepath, cfg,
                        metric_name=metric_name,
                        label_filters=label_filters or None,
                        min_val=args.min,
                        max_val=args.max,
                        tmin_sec=args.tmin,
                        tmax_sec=args.tmax,
                    )

            if use_agg_path and not cfg.no_visuals and agg_series:
                if cfg.plotly:
                    _render_plots_plotly(agg_series, cfg, title_suffix=display_base)
                else:
                    _render_metrics_plotille_agg(
                        agg_series, cfg, display_base, args.min, args.max
                    )
    _t0 = time.perf_counter()
    _mem_cache.clear()
    _elapsed = time.perf_counter() - _t0
    if _elapsed > 0.1:
        _info(f"(cache freed in {_elapsed:.1f}s)")
