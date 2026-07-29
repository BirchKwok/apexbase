#!/usr/bin/env python3
"""Per-public-API latency and memory benchmark for the ApexBase Python SDK.

Each API is measured in an isolated subprocess. Setup allocations are excluded
from incremental figures, while total process RSS is retained in the report so
large mmap/cache footprints remain visible. The public API manifest is also
used by a fast pytest guard to prevent new APIs from landing without a case.
"""

from __future__ import annotations

import argparse
import gc
import importlib.util
import inspect
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import tracemalloc

try:
    import resource
except ImportError:  # Not available on Windows.
    resource = None


if os.name == "nt":
    import ctypes

    class _ProcessMemoryCounters(ctypes.Structure):
        _fields_ = (
            ("cb", ctypes.c_ulong),
            ("page_fault_count", ctypes.c_ulong),
            ("peak_working_set_size", ctypes.c_size_t),
            ("working_set_size", ctypes.c_size_t),
            ("quota_peak_paged_pool_usage", ctypes.c_size_t),
            ("quota_paged_pool_usage", ctypes.c_size_t),
            ("quota_peak_non_paged_pool_usage", ctypes.c_size_t),
            ("quota_non_paged_pool_usage", ctypes.c_size_t),
            ("pagefile_usage", ctypes.c_size_t),
            ("peak_pagefile_usage", ctypes.c_size_t),
        )

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _psapi = ctypes.WinDLL("psapi", use_last_error=True)
    _kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    _psapi.GetProcessMemoryInfo.argtypes = (
        ctypes.c_void_p,
        ctypes.POINTER(_ProcessMemoryCounters),
        ctypes.c_ulong,
    )
    _psapi.GetProcessMemoryInfo.restype = ctypes.c_int


ROWS = 2_000
WARM_ITERATIONS = 7
OPTIONAL_LANCE_APIS = {
    "ApexClient.from_lance",
    "ApexClient.to_lance",
    "ResultView.to_lance",
}

# Every public ApexClient/ResultView member has an explicit case. Dunder entries
# below are the documented sequence/context protocols; implementation helpers
# beginning with an underscore are intentionally outside this SDK benchmark.
PUBLIC_API_CASES = (
    "ApexClient.__init__",
    "ApexClient.use_database",
    "ApexClient.use",
    "ApexClient.current_database",
    "ApexClient.list_databases",
    "ApexClient.use_table",
    "ApexClient.current_table",
    "ApexClient.create_table",
    "ApexClient.drop_table",
    "ApexClient.list_tables",
    "ApexClient.register_temp_table",
    "ApexClient.drop_temp_table",
    "ApexClient.set_compression",
    "ApexClient.get_compression",
    "ApexClient.init_fts",
    "ApexClient.disable_fts",
    "ApexClient.drop_fts",
    "ApexClient.store",
    "ApexClient.store_durable_one",
    "ApexClient.execute",
    "ApexClient.execute_batch",
    "ApexClient.execute_batch_parallel",
    "ApexClient.topk_distance",
    "ApexClient.batch_topk_distance",
    "ApexClient.query",
    "ApexClient.retrieve",
    "ApexClient.read_blob",
    "ApexClient.read_blobs",
    "ApexClient.read_blob_range",
    "ApexClient.read_blob_ranges",
    "ApexClient.read_blob_descriptor",
    "ApexClient.read_blob_info",
    "ApexClient.read_blob_infos",
    "ApexClient.retrieve_many",
    "ApexClient.retrieve_all",
    "ApexClient.list_fields",
    "ApexClient.delete",
    "ApexClient.replace",
    "ApexClient.batch_replace",
    "ApexClient.from_pandas",
    "ApexClient.from_pyarrow",
    "ApexClient.from_lance",
    "ApexClient.from_polars",
    "ApexClient.to_lance",
    "ApexClient.optimize",
    "ApexClient.count_rows",
    "ApexClient.flush",
    "ApexClient.begin_buffered_writes",
    "ApexClient.end_buffered_writes",
    "ApexClient.flush_buffered_writes",
    "ApexClient.buffered_write_count",
    "ApexClient.flush_cache",
    "ApexClient.set_auto_flush",
    "ApexClient.get_auto_flush",
    "ApexClient.estimate_memory_bytes",
    "ApexClient.drop_column",
    "ApexClient.add_column",
    "ApexClient.rename_column",
    "ApexClient.get_column_dtype",
    "ApexClient.search_text",
    "ApexClient.search_text_with_scores",
    "ApexClient.fuzzy_search_text",
    "ApexClient.search_and_retrieve",
    "ApexClient.search_and_retrieve_top",
    "ApexClient.set_fts_fuzzy_config",
    "ApexClient.get_fts_stats",
    "ApexClient.compact_fts_index",
    "ApexClient.warmup_fts_terms",
    "ApexClient.close",
    "ApexClient.create_clean",
    "ApexClient.__enter__",
    "ApexClient.__exit__",
    "ApexClient.__repr__",
    "ResultView.__init__",
    "ResultView.from_arrow_bytes",
    "ResultView.from_dicts",
    "ResultView.to_dict",
    "ResultView.tolist",
    "ResultView.to_pandas",
    "ResultView.to_polars",
    "ResultView.to_arrow",
    "ResultView.to_record_batches",
    "ResultView.iter_batches",
    "ResultView.to_lance",
    "ResultView.shape",
    "ResultView.columns",
    "ResultView.ids",
    "ResultView.get_ids",
    "ResultView.scalar",
    "ResultView.first",
    "ResultView.__len__",
    "ResultView.__iter__",
    "ResultView.__getitem__",
    "ResultView.__repr__",
    "encode_vector",
    "decode_vector",
)


def discover_public_api_names():
    """Return the SDK surface that must remain covered by this benchmark."""
    from apexbase import ResultView
    from apexbase.client import ApexClient

    names = {"ApexClient.__init__", "ResultView.__init__", "encode_vector", "decode_vector"}
    for cls in (ApexClient, ResultView):
        for name, value in inspect.getmembers(cls):
            if name.startswith("_"):
                continue
            if callable(value) or isinstance(inspect.getattr_static(cls, name), property):
                names.add(f"{cls.__name__}.{name}")
    names.update(
        f"ApexClient.{name}" for name in ("__enter__", "__exit__", "__repr__")
    )
    names.update(
        f"ResultView.{name}" for name in ("__len__", "__iter__", "__getitem__", "__repr__")
    )
    return names


def _rss_mb():
    if os.name == "nt":
        counters = _ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        if not _psapi.GetProcessMemoryInfo(
            _kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
        ):
            raise ctypes.WinError(ctypes.get_last_error())
        mb = 1024.0 * 1024.0
        return counters.working_set_size / mb, counters.peak_working_set_size / mb
    if sys.platform == "darwin":
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak = value / (1024.0 * 1024.0)
        # ru_maxrss is peak bytes on macOS, so use ps for current RSS.
        try:
            out = subprocess.check_output(
                ["ps", "-o", "rss=", "-p", str(os.getpid())], text=True
            )
            return int(out.strip()) / 1024.0, peak
        except (OSError, subprocess.SubprocessError, ValueError):
            # Sandboxed macOS environments may block process inspection.
            # Peak RSS is still a real OS measurement and is a safe current-RSS
            # upper bound for memory regression reporting.
            return peak, peak
    current = 0.0
    with open(f"/proc/{os.getpid()}/status", encoding="utf-8") as handle:
        for line in handle:
            if line.startswith("VmRSS:"):
                current = int(line.split()[1]) / 1024.0
                break
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    return current, peak


def _columnar(n=ROWS):
    return {
        "value": list(range(n)),
        "score": [float(i % 101) for i in range(n)],
        "category": [f"cat_{i % 20}" for i in range(n)],
        "content": [f"apexbase document number {i} category {i % 20}" for i in range(n)],
    }


class Fixture:
    def __init__(self, root, api):
        import numpy as np
        import pyarrow as pa
        from apexbase import ApexClient, ResultView

        self.api = api
        self.root = Path(root)
        self.np = np
        self.pa = pa
        self.ApexClient = ApexClient
        self.ResultView = ResultView
        self.client = None
        self.extra_clients = []
        self.repeatable = True
        self.expected_error = False
        self._counter = 0
        self.prepared = {}

        if api in {"encode_vector", "decode_vector"} or api.startswith("ResultView."):
            self.arrow = pa.table(
                {
                    "_id": list(range(1, ROWS + 1)),
                    "value": list(range(ROWS)),
                    "score": [float(i) for i in range(ROWS)],
                    "category": [f"cat_{i % 20}" for i in range(ROWS)],
                }
            )
            return
        if api == "ApexClient.__init__":
            return

        self.client = ApexClient(str(self.root / "db"), drop_if_exists=True)
        self.client.create_table(
            "main",
            {"value": "int64", "score": "float64", "category": "string", "content": "string"},
        )
        self.client.store(_columnar())
        self.client.flush()
        self._prepare_client_api(api.split(".", 1)[1])

    def _fresh_table(self, prefix, schema=None):
        self._counter += 1
        name = f"{prefix}_{self._counter}"
        self.client.create_table(name, schema)
        return name

    def _blob_ready(self):
        name = self._fresh_table("blobs", {"payload": "blob", "name": "string"})
        payload = b"apexbase-memory-benchmark:" * 4096
        self.client.store({"payload": [payload, payload + b"2"], "name": ["a", "b"]})
        self.client.flush()
        return payload

    def _vector_ready(self):
        name = self._fresh_table("vectors")
        rng = self.np.random.default_rng(42)
        vectors = rng.random((ROWS, 16), dtype=self.np.float32)
        self.client.store({"value": list(range(ROWS)), "vec": [row for row in vectors]})
        self.client.flush()
        return vectors

    def _fts_ready(self):
        name = self._fresh_table("fts", {"content": "string", "value": "int64"})
        self.client.init_fts(index_fields=["content"])
        self.client.store(
            {
                "content": [f"apexbase searchable document {i}" for i in range(256)],
                "value": list(range(256)),
            }
        )
        return name

    def _prepare_client_api(self, name):
        c = self.client
        if name in {"register_temp_table", "drop_temp_table"}:
            csv = self.root / "sample.csv"
            csv.write_text("value,name\n1,a\n2,b\n", encoding="utf-8")
            self.prepared["csv"] = csv
            if name == "drop_temp_table":
                c.register_temp_table("csv_temp", str(csv))
        elif name == "drop_table":
            self._fresh_table("drop")
        elif name == "set_compression":
            self._fresh_table("compression", {"value": "int64"})
        elif name in {"init_fts", "disable_fts", "drop_fts"}:
            self._fresh_table("fts_control", {"content": "string"})
            if name != "init_fts":
                c.init_fts(index_fields=["content"])
        elif name == "store":
            self._fresh_table(
                "store",
                {"value": "int64", "score": "float64", "category": "string", "content": "string"},
            )
            self.prepared["data"] = _columnar()
        elif name == "store_durable_one":
            self._fresh_table("durable", {"value": "int64"})
        elif name in {"topk_distance", "batch_topk_distance"}:
            self.prepared["vectors"] = self._vector_ready()
        elif name.startswith("read_blob"):
            self.prepared["payload"] = self._blob_ready()
        elif name in {"retrieve", "retrieve_many", "retrieve_all", "list_fields", "delete", "replace", "batch_replace", "execute", "execute_batch", "execute_batch_parallel", "query", "count_rows"}:
            c.use_table("main")
        elif name in {"from_pandas", "from_pyarrow", "from_polars", "from_lance"}:
            data = _columnar()
            table = self.pa.table(data)
            self.prepared["arrow"] = table
            if name == "from_pandas":
                import pandas as pd
                self.prepared["input"] = pd.DataFrame(data)
            elif name == "from_polars":
                import polars as pl
                self.prepared["input"] = pl.from_arrow(table)
            elif name == "from_lance":
                import lance
                source = self.root / "source.lance"
                lance.write_dataset(table, str(source))
                self.prepared["input"] = str(source)
            else:
                self.prepared["input"] = table
        elif name == "to_lance":
            c.use_table("main")
        elif name in {"optimize", "flush", "flush_cache"}:
            c.use_table("main")
            c.store({"value": ROWS + 1, "score": 1.0, "category": "cat", "content": "pending"})
        elif name == "begin_buffered_writes":
            c.use_table("main")
        elif name == "end_buffered_writes":
            c.use_table("main")
            c.begin_buffered_writes()
            c.store({"value": ROWS + 2, "score": 1.0, "category": "cat", "content": "buffered"})
        elif name == "flush_buffered_writes":
            c.use_table("main")
            c.begin_buffered_writes()
            for i in range(128):
                c.store({"value": ROWS + i, "score": 1.0, "category": "cat", "content": "buffered"})
        elif name == "buffered_write_count":
            c.use_table("main")
            c.begin_buffered_writes()
            c.store({"value": ROWS + 1, "score": 1.0, "category": "cat", "content": "buffered"})
        elif name in {"add_column", "drop_column", "rename_column", "get_column_dtype"}:
            self._fresh_table("columns", {"value": "int64", "extra": "string"})
        elif name in {"search_text", "search_text_with_scores", "fuzzy_search_text", "search_and_retrieve", "search_and_retrieve_top", "get_fts_stats", "compact_fts_index", "warmup_fts_terms", "set_fts_fuzzy_config"}:
            self._fts_ready()

    def result_view(self):
        return self.ResultView(arrow_table=self.arrow)

    def call(self):
        api = self.api
        if api == "encode_vector":
            from apexbase.client import encode_vector
            return encode_vector(self.np.arange(4096, dtype=self.np.float32))
        if api == "decode_vector":
            from apexbase.client import encode_vector, decode_vector
            return decode_vector(encode_vector(self.np.arange(4096, dtype=self.np.float32)))
        if api.startswith("ResultView."):
            return self._call_result_view(api.split(".", 1)[1])
        if api == "ApexClient.__init__":
            self.repeatable = False
            client = self.ApexClient(str(self.root / "constructor"), drop_if_exists=True)
            self.extra_clients.append(client)
            return client
        return self._call_client(api.split(".", 1)[1])

    def _call_result_view(self, name):
        rv = self.result_view()
        if name == "__init__":
            return self.ResultView(arrow_table=self.arrow)
        if name in {"from_arrow_bytes", "from_dicts"}:
            self.expected_error = True
            try:
                return getattr(self.ResultView, name)(b"" if name == "from_arrow_bytes" else [])
            except RuntimeError:
                return None
        if name == "to_lance":
            self.repeatable = False
            return rv.to_lance(str(self.root / "result.lance"))
        if name == "shape" or name == "columns" or name == "ids":
            return getattr(rv, name)
        if name == "get_ids":
            return rv.get_ids()
        if name == "scalar":
            return rv.scalar()
        if name == "first":
            return rv.first()
        if name == "__len__":
            return len(rv)
        if name == "__iter__":
            return iter(rv)
        if name == "__getitem__":
            return rv[0]
        if name == "__repr__":
            return repr(rv)
        return getattr(rv, name)()

    def _call_client(self, name):
        c = self.client
        if name == "use_database":
            return c.use_database("benchdb")
        if name == "use":
            return c.use("benchuse", "items")
        if name in {"current_database", "current_table"}:
            return getattr(c, name)
        if name == "list_databases" or name == "list_tables":
            return getattr(c, name)()
        if name == "use_table":
            return c.use_table("main")
        if name == "create_table":
            self.repeatable = False
            return c.create_table("created", {"value": "int64"})
        if name == "drop_table":
            self.repeatable = False
            return c.drop_table(c.current_table)
        if name in {"register_temp_table", "drop_temp_table"}:
            c.use_table("main")
            if name == "register_temp_table":
                self.repeatable = False
                return c.register_temp_table("csv_temp", str(self.prepared["csv"]))
            self.repeatable = False
            return c.drop_temp_table("csv_temp")
        if name == "set_compression":
            return c.set_compression("lz4")
        if name == "get_compression":
            return c.get_compression()
        if name in {"init_fts", "disable_fts", "drop_fts"}:
            self.repeatable = False
            return getattr(c, name)()
        if name == "store":
            self.repeatable = False
            return c.store(self.prepared["data"])
        if name == "store_durable_one":
            self.repeatable = False
            return c.store_durable_one({"value": 1})
        if name == "execute":
            c.use_table("main")
            return c.execute("SELECT category, SUM(value) FROM main GROUP BY category")
        if name == "execute_batch":
            c.use_table("main")
            return c.execute_batch(["SELECT COUNT(*) FROM main", "SELECT MAX(value) FROM main"])
        if name == "execute_batch_parallel":
            c.use_table("main")
            return c.execute_batch_parallel(
                ["SELECT COUNT(*) FROM main", "SELECT MAX(value) FROM main"]
            )
        if name in {"topk_distance", "batch_topk_distance"}:
            vectors = self.prepared["vectors"]
            if name == "topk_distance":
                return c.topk_distance("vec", vectors[0], k=10)
            return c.batch_topk_distance("vec", vectors[:8], k=10)
        if name == "query":
            c.use_table("main")
            return c.query(where_clause="value >= 100", limit=100)
        if name == "retrieve":
            c.use_table("main")
            return c.retrieve(1)
        if name.startswith("read_blob"):
            if name == "read_blob":
                return c.read_blob("payload", 1)
            if name == "read_blobs":
                return c.read_blobs("payload", [1, 2])
            if name == "read_blob_range":
                return c.read_blob_range("payload", 1, 128, 4096)
            if name == "read_blob_ranges":
                return c.read_blob_ranges("payload", [1, 2], [128, 256], 4096)
            if name == "read_blob_descriptor":
                return c.read_blob_descriptor("payload", 1)
            if name == "read_blob_info":
                return c.read_blob_info("payload", 1)
            return c.read_blob_infos("payload", [1, 2])
        if name == "retrieve_many":
            c.use_table("main")
            return c.retrieve_many(list(range(1, 101)))
        if name == "retrieve_all":
            c.use_table("main")
            return c.retrieve_all()
        if name == "list_fields":
            c.use_table("main")
            return c.list_fields()
        if name == "delete":
            c.use_table("main")
            self.repeatable = False
            return c.delete(id=ROWS)
        if name == "replace":
            c.use_table("main")
            return c.replace(1, {"value": 1, "score": 1.0, "category": "cat_1", "content": "replacement"})
        if name == "batch_replace":
            c.use_table("main")
            return c.batch_replace({1: {"value": 1}, 2: {"value": 2}})
        if name in {"from_pandas", "from_pyarrow", "from_polars", "from_lance"}:
            self.repeatable = False
            if name == "from_pandas":
                return c.from_pandas(self.prepared["input"], table_name="from_pandas")
            if name == "from_pyarrow":
                return c.from_pyarrow(self.prepared["input"], table_name="from_pyarrow")
            if name == "from_polars":
                return c.from_polars(self.prepared["input"], table_name="from_polars")
            return c.from_lance(self.prepared["input"], table_name="from_lance")
        if name == "to_lance":
            c.use_table("main")
            self.repeatable = False
            return c.to_lance(str(self.root / "client.lance"))
        if name in {"optimize", "flush", "flush_cache"}:
            self.repeatable = False
            return getattr(c, name)()
        if name == "count_rows":
            c.use_table("main")
            return c.count_rows()
        if name == "begin_buffered_writes":
            return c.begin_buffered_writes()
        if name == "end_buffered_writes":
            self.repeatable = False
            return c.end_buffered_writes()
        if name == "flush_buffered_writes":
            self.repeatable = False
            return c.flush_buffered_writes()
        if name == "buffered_write_count":
            return c.buffered_write_count()
        if name == "set_auto_flush":
            return c.set_auto_flush(rows=1000, bytes=1 << 20)
        if name == "get_auto_flush":
            return c.get_auto_flush()
        if name == "estimate_memory_bytes":
            return c.estimate_memory_bytes()
        if name in {"add_column", "drop_column", "rename_column", "get_column_dtype"}:
            self.repeatable = False
            if name == "add_column":
                return c.add_column("added", "int64")
            if name == "drop_column":
                return c.drop_column("extra")
            if name == "rename_column":
                return c.rename_column("extra", "renamed")
            return c.get_column_dtype("value")
        if name in {"search_text", "search_text_with_scores", "fuzzy_search_text", "search_and_retrieve", "search_and_retrieve_top", "get_fts_stats", "compact_fts_index", "warmup_fts_terms", "set_fts_fuzzy_config"}:
            if name == "search_text":
                return c.search_text("apexbase")
            if name == "search_text_with_scores":
                return c.search_text_with_scores("apexbase")
            if name == "fuzzy_search_text":
                return c.fuzzy_search_text("apexbas")
            if name == "search_and_retrieve":
                return c.search_and_retrieve("apexbase", limit=100)
            if name == "search_and_retrieve_top":
                return c.search_and_retrieve_top("apexbase", n=100)
            if name == "warmup_fts_terms":
                return c.warmup_fts_terms(["apexbase", "document"])
            if name == "set_fts_fuzzy_config":
                return c.set_fts_fuzzy_config()
            return getattr(c, name)()
        if name == "close":
            self.repeatable = False
            return c.close()
        if name == "create_clean":
            self.repeatable = False
            client = self.ApexClient.create_clean(str(self.root / "clean"))
            self.extra_clients.append(client)
            return client
        if name == "__enter__":
            return c.__enter__()
        if name == "__exit__":
            self.repeatable = False
            return c.__exit__(None, None, None)
        if name == "__repr__":
            return repr(c)
        raise KeyError(f"No benchmark invocation for {self.api}")

    def cleanup(self):
        for client in self.extra_clients:
            try:
                client.close()
            except Exception:
                pass
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass


def _measure_worker(api):
    if api in OPTIONAL_LANCE_APIS and importlib.util.find_spec("lance") is None:
        return {"api": api, "status": "skipped", "reason": "lance is not installed"}

    # Time first without allocation tracing or RSS sampling. Memory is measured
    # with a second, clean fixture so even one-shot mutating APIs get an honest
    # latency number and a cold memory profile.
    with tempfile.TemporaryDirectory(prefix="apexbase-public-api-time-") as tmp:
        timing_fixture = Fixture(tmp, api)
        gc.collect()
        started = time.perf_counter_ns()
        try:
            result = timing_fixture.call()
            cold_ms = (time.perf_counter_ns() - started) / 1_000_000.0
            status = "ok"
            error = None
        except Exception as exc:
            cold_ms = (time.perf_counter_ns() - started) / 1_000_000.0
            result = None
            status = "error"
            error = f"{type(exc).__name__}: {exc}"

        warm_ms = None
        if status == "ok" and timing_fixture.repeatable:
            timings = []
            for _ in range(WARM_ITERATIONS):
                begin = time.perf_counter_ns()
                result = timing_fixture.call()
                timings.append((time.perf_counter_ns() - begin) / 1_000_000.0)
            warm_ms = statistics.median(timings)
        _ = result
        timing_fixture.cleanup()

    if status != "ok":
        return {
            "api": api,
            "status": status,
            "cold_ms": round(cold_ms, 6),
            "warm_median_ms": None,
            "error": error,
        }

    with tempfile.TemporaryDirectory(prefix="apexbase-public-api-memory-") as tmp:
        fixture = Fixture(tmp, api)
        gc.collect()
        rss_before, process_peak_before = _rss_mb()
        samples = [rss_before]
        stop = threading.Event()

        def sample_rss():
            while not stop.wait(0.001):
                try:
                    samples.append(_rss_mb()[0])
                except Exception:
                    return

        sampler = threading.Thread(target=sample_rss, daemon=True)
        tracemalloc.start()
        sampler.start()
        try:
            result = fixture.call()
            current_py, peak_py = tracemalloc.get_traced_memory()
        except Exception as exc:
            current_py, peak_py = tracemalloc.get_traced_memory()
            result = None
            status = "error"
            error = f"{type(exc).__name__}: {exc}"
        finally:
            stop.set()
            sampler.join(timeout=0.2)
            tracemalloc.stop()
        rss_after, process_peak_after = _rss_mb()
        samples.append(rss_after)

        # Keep result alive through RSS sampling so result materialization is counted.
        _ = result
        fixture.cleanup()
        return {
            "api": api,
            "status": status,
            "cold_ms": round(cold_ms, 6),
            "warm_median_ms": None if warm_ms is None else round(warm_ms, 6),
            "rss_before_mb": round(rss_before, 3),
            "rss_after_mb": round(rss_after, 3),
            "rss_peak_mb": round(max(max(samples), process_peak_after), 3),
            "rss_peak_delta_mb": round(
                max(0.0, max(max(samples), process_peak_after) - max(rss_before, process_peak_before)), 3
            ),
            "rss_retained_delta_mb": round(rss_after - rss_before, 3),
            "python_peak_kb": round(peak_py / 1024.0, 3),
            "python_retained_kb": round(current_py / 1024.0, 3),
            "error": error,
        }


def _print_table(results):
    print(
        f"{'Public API':46} {'cold ms':>10} {'warm ms':>10} "
        f"{'RSS peak':>10} {'RSS +MB':>9} {'Py peak KB':>11}"
    )
    print("-" * 104)
    for row in results:
        if row["status"] != "ok":
            print(f"{row['api']:46} {row['status']:>10}  {row.get('reason') or row.get('error')}")
            continue
        warm = "-" if row["warm_median_ms"] is None else f"{row['warm_median_ms']:.3f}"
        print(
            f"{row['api']:46} {row['cold_ms']:10.3f} {warm:>10} "
            f"{row['rss_peak_mb']:10.1f} {row['rss_peak_delta_mb']:9.1f} "
            f"{row['python_peak_kb']:11.1f}"
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", choices=PUBLIC_API_CASES)
    parser.add_argument("--api", action="append", choices=PUBLIC_API_CASES)
    parser.add_argument("--json", type=Path)
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    if args.list:
        print("\n".join(PUBLIC_API_CASES))
        return 0
    if args.worker:
        print(json.dumps(_measure_worker(args.worker), sort_keys=True))
        return 0

    selected = args.api or list(PUBLIC_API_CASES)
    results = []
    for index, api in enumerate(selected, 1):
        proc = subprocess.run(
            [sys.executable, __file__, "--worker", api],
            check=False,
            capture_output=True,
            text=True,
        )
        if proc.returncode:
            row = {"api": api, "status": "error", "error": proc.stderr.strip()}
        else:
            try:
                row = json.loads(proc.stdout.strip().splitlines()[-1])
            except (IndexError, json.JSONDecodeError) as exc:
                row = {"api": api, "status": "error", "error": f"invalid worker output: {exc}"}
        results.append(row)
        print(f"[{index:02d}/{len(selected):02d}] {api}: {row['status']}", file=sys.stderr)

    _print_table(results)
    payload = {
        "metadata": {
            "rows": ROWS,
            "warm_iterations": WARM_ITERATIONS,
            "python": sys.version.split()[0],
            "platform": sys.platform,
            "api_count": len(selected),
        },
        "results": results,
    }
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        print(f"\nJSON report: {args.json}")
    return 1 if any(row["status"] == "error" for row in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
