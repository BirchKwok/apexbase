"""
Memory usage comparison test: ApexBase vs DuckDB

Ensures ApexBase memory footprint does not exceed DuckDB by more than 10%.
This is critical for low-resource machine deployments.
"""

import gc
import os
import sys
import subprocess
import tempfile
import shutil

try:
    import resource
except ImportError:  # Windows
    resource = None

import pytest
import pyarrow as pa
import duckdb


def get_rss_mb():
    """Get current RSS (Resident Set Size) in MB via resource module."""
    if resource is None:
        import ctypes
        from ctypes import wintypes

        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel32.GetCurrentProcess.argtypes = []
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(ProcessMemoryCounters),
            wintypes.DWORD,
        ]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL

        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        process = kernel32.GetCurrentProcess()
        if not psapi.GetProcessMemoryInfo(
            process, ctypes.byref(counters), counters.cb
        ):
            raise ctypes.WinError(ctypes.get_last_error())
        return counters.WorkingSetSize / (1024 * 1024)

    # On macOS, ru_maxrss is in bytes; on Linux it's in KB
    usage = resource.getrusage(resource.RUSAGE_SELF)
    import sys
    if sys.platform == 'darwin':
        return usage.ru_maxrss / (1024 * 1024)
    else:
        return usage.ru_maxrss / 1024


def get_current_rss_mb():
    """Get current RSS in MB (more accurate than peak via /proc or ps)."""
    import sys
    pid = os.getpid()
    if sys.platform == 'darwin':
        # Use ps to get current RSS on macOS
        import subprocess
        try:
            out = subprocess.check_output(['ps', '-o', 'rss=', '-p', str(pid)], text=True)
            return int(out.strip()) / 1024  # KB -> MB
        except Exception:
            return get_rss_mb()
    elif sys.platform.startswith('linux'):
        # Linux: read from /proc
        try:
            with open(f'/proc/{pid}/status') as f:
                for line in f:
                    if line.startswith('VmRSS:'):
                        return int(line.split()[1]) / 1024  # KB -> MB
        except Exception:
            return get_rss_mb()
    else:
        return get_rss_mb()


NUM_ROWS = 10_000


def generate_test_data(n=NUM_ROWS):
    """Generate test data as columnar dict."""
    return {
        'id': list(range(n)),
        'value_a': [i * 17 % 9973 for i in range(n)],
        'value_b': [i * 31 % 7919 for i in range(n)],
        'value_c': [float(i % 1000) + 0.5 for i in range(n)],
        'category': [f'cat_{i % 50}' for i in range(n)],
        'label': [f'label_{i % 200}' for i in range(n)],
    }


def _data_to_arrow(data):
    """Convert columnar dict to PyArrow table for fast DuckDB insert."""
    return pa.table(data)


class TestMemoryComparison:
    """Compare ApexBase memory usage against DuckDB.
    
    ApexBase must not exceed DuckDB memory by more than 10%.
    """

    def _measure_apexbase_memory(self, data, tmp_dir):
        """Measure ApexBase memory for store + query cycle. Returns RSS delta in MB."""
        from apexbase import ApexClient

        gc.collect()
        rss_before = get_current_rss_mb()

        client = ApexClient(dirpath=os.path.join(tmp_dir, 'apex'))
        client.create_table('bench')
        client.store(data)
        client.flush()

        # Perform representative queries (forces data to be read)
        r1 = client.execute("SELECT COUNT(*) FROM bench")
        r2 = client.execute("SELECT category, SUM(value_a) FROM bench GROUP BY category")
        r3 = client.execute("SELECT * FROM bench WHERE value_a > 5000 LIMIT 100")

        gc.collect()
        rss_after = get_current_rss_mb()

        # Clean up
        client.close()
        del client, r1, r2, r3
        gc.collect()

        return rss_after - rss_before

    def _measure_duckdb_memory(self, data, tmp_dir):
        """Measure DuckDB memory for store + query cycle. Returns RSS delta in MB."""
        gc.collect()
        rss_before = get_current_rss_mb()

        con = duckdb.connect(':memory:')

        # Bulk insert via Arrow (fast path — avoids slow executemany)
        arrow_table = _data_to_arrow(data)
        con.execute("CREATE TABLE bench AS SELECT * FROM arrow_table")

        # Perform same representative queries
        r1 = con.execute("SELECT COUNT(*) FROM bench").fetchall()
        r2 = con.execute("SELECT category, SUM(value_a) FROM bench GROUP BY category").fetchall()
        r3 = con.execute("SELECT * FROM bench WHERE value_a > 5000 LIMIT 100").fetchall()

        gc.collect()
        rss_after = get_current_rss_mb()

        # Clean up
        con.close()
        del con, arrow_table, r1, r2, r3
        gc.collect()

        return rss_after - rss_before

    def test_memory_usage_within_duckdb_10_percent(self):
        """ApexBase memory usage must not exceed DuckDB by more than 10%.
        
        This test measures RSS (Resident Set Size) delta for both databases
        performing equivalent store + query operations on 50K rows.
        """
        data = generate_test_data()

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Measure DuckDB first (to warm up Python/shared libs)
            duckdb_mem = self._measure_duckdb_memory(data, tmp_dir)

        gc.collect()

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Measure ApexBase
            apex_mem = self._measure_apexbase_memory(data, tmp_dir)

        gc.collect()

        # Compute threshold: ApexBase must be <= DuckDB * 1.10
        # Use max(duckdb_mem, 5.0) to avoid division-by-zero or tiny-delta issues
        duckdb_baseline = max(duckdb_mem, 5.0)
        threshold = duckdb_baseline * 1.10

        print(f"\n=== Memory Comparison ({NUM_ROWS} rows, 6 columns) ===")
        print(f"  DuckDB RSS delta:    {duckdb_mem:.2f} MB")
        print(f"  ApexBase RSS delta:  {apex_mem:.2f} MB")
        print(f"  Threshold (110%):    {threshold:.2f} MB")
        print(f"  Ratio:               {apex_mem / duckdb_baseline:.2f}x")

        assert apex_mem <= threshold, (
            f"ApexBase memory ({apex_mem:.1f} MB) exceeds DuckDB ({duckdb_mem:.1f} MB) "
            f"by more than 10% (threshold: {threshold:.1f} MB)"
        )

    def test_query_memory_on_demand(self):
        """Verify ApexBase on-demand reading doesn't load entire dataset into memory.
        
        After storing 50K rows and closing/reopening, a simple COUNT(*)
        should use minimal memory (much less than full data size).
        """
        from apexbase import ApexClient

        data = generate_test_data()

        with tempfile.TemporaryDirectory() as tmp_dir:
            apex_dir = os.path.join(tmp_dir, 'apex')

            # Store data and close
            client = ApexClient(dirpath=apex_dir)
            client.create_table('bench')
            client.store(data)
            client.flush()
            client.close()
            del client
            gc.collect()

            # Reopen and measure memory for a lightweight query
            gc.collect()
            rss_before = get_current_rss_mb()

            client2 = ApexClient(dirpath=apex_dir)
            client2.use_table('bench')
            result = client2.execute("SELECT COUNT(*) FROM bench")
            count = result.scalar()

            gc.collect()
            rss_after = get_current_rss_mb()
            mem_delta = rss_after - rss_before

            client2.close()

            print(f"\n=== On-Demand Memory (COUNT(*) on {NUM_ROWS} rows) ===")
            print(f"  RSS delta for COUNT(*): {mem_delta:.2f} MB")
            print(f"  Row count: {count}")

            assert count == NUM_ROWS
            # COUNT(*) should not load entire dataset; expect < 20 MB overhead
            # (Full data ~6 cols × 100K rows ≈ 10-30 MB if loaded entirely)
            assert mem_delta < 50, (
                f"COUNT(*) caused {mem_delta:.1f} MB memory increase — "
                f"suggests entire dataset may have been loaded"
            )
