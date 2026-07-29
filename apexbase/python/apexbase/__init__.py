"""
ApexBase - High-performance embedded database based on Rust core

Uses custom single-file storage format (.apex) to provide efficient data storage and query functionality.
"""

from __future__ import annotations

import importlib.util
import weakref
import atexit
from typing import List, Optional, Literal

import numpy as np

# Import Rust core
from apexbase._core import ApexStorage, __version__

# FTS is now directly implemented in Rust layer, no need for Python nanofts package
# But keep compatibility flag
FTS_AVAILABLE = True  # Always available since integrated into Rust core

# Optional data framework support is detected eagerly but imported lazily.
pa = None
pd = None
pl = None
ARROW_AVAILABLE = importlib.util.find_spec("pyarrow") is not None
PANDAS_AVAILABLE = importlib.util.find_spec("pandas") is not None
POLARS_AVAILABLE = importlib.util.find_spec("polars") is not None


def _ensure_pyarrow():
    global pa, ARROW_AVAILABLE
    if pa is None:
        if not ARROW_AVAILABLE:
            raise ImportError("pyarrow not available. Install with: pip install pyarrow")
        import pyarrow as _pa
        pa = _pa
    return pa


def _ensure_pandas():
    global pd, PANDAS_AVAILABLE
    if pd is None:
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas not available. Install with: pip install pandas")
        import pandas as _pd
        pd = _pd
    return pd


def _ensure_polars():
    global pl, POLARS_AVAILABLE
    if pl is None:
        if not POLARS_AVAILABLE:
            raise ImportError("polars not available. Install with: pip install polars")
        import polars as _pl
        pl = _pl
    return pl


def _ensure_lance():
    if importlib.util.find_spec("lance") is None:
        raise ImportError("lance not available. Install with: pip install pylance")
    import lance
    return lance

__version__ = "1.25.0"


class _InstanceRegistry:
    """Global instance registry with multi-client support
    
    This registry now supports multiple clients per database path using reference counting.
    The underlying storage is shared among all clients for the same database.
    """
    
    def __init__(self):
        # db_path -> {'storage': ApexStorage, 'clients': {client_id: weakref}, 'count': int, 'storage_lock': threading.RLock}
        self._instances = {}
        self._lock = None
        self._client_id_counter = 0
        self._client_id_lock = None
    
    def _get_storage_lock(self, db_path: str):
        """Get or create a lock for the storage at the given path"""
        lock = self._get_lock()
        with lock:
            if db_path in self._instances:
                entry = self._instances[db_path]
                if 'storage_lock' not in entry:
                    import threading
                    entry['storage_lock'] = threading.RLock()
                return entry['storage_lock']
        return None
    
    def _get_lock(self):
        if self._lock is None:
            import threading
            self._lock = threading.RLock()
        return self._lock
    
    def _get_client_id_lock(self):
        if self._client_id_lock is None:
            import threading
            self._client_id_lock = threading.Lock()
        return self._client_id_lock
    
    def _generate_client_id(self):
        """Generate a unique client ID"""
        with self._get_client_id_lock():
            self._client_id_counter += 1
            return self._client_id_counter
    
    def register(self, instance, db_path: str):
        """Register a new client for the given database path.
        
        If other active clients exist for this path, share the underlying storage.
        If all existing clients are closed, create new storage.
        """
        lock = self._get_lock()
        with lock:
            if db_path in self._instances:
                entry = self._instances[db_path]
                
                # Check if any existing clients are still active
                active_clients = []
                for cid, client_ref in list(entry['clients'].items()):
                    client = client_ref()
                    if client is not None and not getattr(client, '_is_closed', False):
                        active_clients.append(cid)
                
                if active_clients:
                    # Another active client exists - share the storage
                    client_id = self._generate_client_id()
                    entry['clients'][client_id] = weakref.ref(instance, 
                        lambda ref: self._cleanup_ref(db_path, client_id, ref))
                    entry['count'] += 1
                    # Attach shared storage to the new instance
                    instance._shared_storage = entry['storage']
                    instance._client_id = client_id
                    instance._is_shared_client = True
                else:
                    # All previous clients are closed - create new storage
                    client_id = self._generate_client_id()
                    import threading
                    self._instances[db_path] = {
                        'storage': None,  # Will be set by the client
                        'clients': {client_id: weakref.ref(instance)},
                        'count': 1,
                        'storage_lock': threading.RLock()
                    }
                    instance._client_id = client_id
                    instance._is_shared_client = False
            else:
                # First client for this database - create new storage
                client_id = self._generate_client_id()
                import threading
                self._instances[db_path] = {
                    'storage': None,  # Will be set by the client
                    'clients': {client_id: weakref.ref(instance)},
                    'count': 1,
                    'storage_lock': threading.RLock()
                }
                instance._client_id = client_id
                instance._is_shared_client = False
    
    def set_storage(self, db_path: str, storage):
        """Set the storage instance for a database (called by client after creating storage)"""
        lock = self._get_lock()
        with lock:
            if db_path in self._instances:
                self._instances[db_path]['storage'] = storage
    
    def _cleanup_ref(self, db_path: str, client_id: int, ref):
        """Clean up when a client is garbage collected"""
        lock = self._get_lock()
        with lock:
            if db_path in self._instances:
                entry = self._instances[db_path]
                if client_id in entry['clients']:
                    del entry['clients'][client_id]
                    entry['count'] -= 1
                    
                    # If no more clients, clean up
                    if entry['count'] <= 0:
                        self._instances.pop(db_path, None)
    
    def unregister(self, db_path: str, client_id: int = None):
        """Unregister a client and return storage when this was the last client."""
        lock = self._get_lock()
        with lock:
            if db_path in self._instances:
                entry = self._instances[db_path]
                if client_id and client_id in entry['clients']:
                    del entry['clients'][client_id]
                    entry['count'] -= 1
                    
                    # If no more clients, the storage will be closed by the client's close() method
                    if entry['count'] <= 0:
                        removed = self._instances.pop(db_path, None)
                        return removed.get('storage') if removed else None
                elif not client_id:
                    # Remove all clients for this path
                    removed = self._instances.pop(db_path, None)
                    return removed.get('storage') if removed else None
            return None
    
    def get_storage(self, db_path: str):
        """Get the shared storage for a database path"""
        lock = self._get_lock()
        with lock:
            if db_path in self._instances:
                return self._instances[db_path]['storage']
            return None
    
    def get_storage_lock(self, db_path: str):
        """Get the lock for the shared storage (for thread-safe concurrent access)"""
        lock = self._get_lock()
        with lock:
            if db_path in self._instances:
                return self._instances[db_path].get('storage_lock')
            return None
    
    def close_all(self):
        """Close all instances (called at program exit)"""
        lock = self._get_lock()
        storages = []
        with lock:
            # First, mark all clients as closed
            for db_path, entry in list(self._instances.items()):
                for client_ref in list(entry['clients'].values()):
                    client = client_ref()
                    if client is not None:
                        try:
                            client._is_closed = True
                            client._storage = None
                        except Exception:
                            pass
            for db_path, entry in list(self._instances.items()):
                storage = entry.get('storage')
                if storage is not None:
                    storages.append(storage)
            self._instances.clear()
        for storage in storages:
            try:
                storage.close()
            except Exception:
                pass


_registry = _InstanceRegistry()
atexit.register(_registry.close_all)


class ResultView:
    """Query result view - Arrow-first high-performance implementation"""
    
    def __init__(self, arrow_table=None, data=None, lazy_pydict=None):
        """
        Initialize ResultView (Arrow-first mode)
        
        Args:
            arrow_table: PyArrow Table (primary data source, fastest)
            data: List[dict] data (optional, for fallback)
            lazy_pydict: dict of column_name -> list (deferred Arrow creation)
        """
        self._arrow_table = arrow_table
        self._data = data  # Lazy loading, convert from Arrow
        self._lazy_pydict = lazy_pydict  # Deferred: convert to Arrow on demand
        if arrow_table is not None:
            self._num_rows = arrow_table.num_rows
        elif lazy_pydict is not None:
            first_col = next(iter(lazy_pydict.values()), [])
            self._num_rows = len(first_col)
        elif data is not None:
            self._num_rows = len(data)
        else:
            self._num_rows = 0
    
    @classmethod
    def from_arrow_bytes(cls, arrow_bytes: bytes) -> 'ResultView':
        raise RuntimeError("Arrow IPC bytes path has been removed. Use Arrow FFI results only.")
    
    @classmethod
    def from_dicts(cls, data: List[dict]) -> 'ResultView':
        raise RuntimeError("Non-Arrow query path has been removed. Use Arrow FFI results only.")
    
    def _ensure_arrow(self):
        """Materialize Arrow table from lazy_pydict if needed"""
        if self._arrow_table is None and self._lazy_pydict is not None:
            pa_mod = _ensure_pyarrow()
            self._arrow_table = pa_mod.Table.from_pydict(self._lazy_pydict)
            self._lazy_pydict = None
    
    def _ensure_data(self):
        """Ensure _data is available (lazy load from Arrow conversion, optionally hide _id)"""
        if self._data is None:
            # Try lazy pydict first (avoids Arrow round-trip for to_dict)
            if self._lazy_pydict is not None:
                show_id = bool(getattr(self, "_show_internal_id", False))
                d = self._lazy_pydict
                keys = [k for k in d if show_id or k != '_id']
                cols = [d[k] for k in keys]
                n = len(next(iter(d.values()), []))
                # Common SQL result widths benefit from avoiding a per-row inner
                # comprehension over column names during Python row materialization.
                if len(keys) == 6:
                    k0, k1, k2, k3, k4, k5 = keys
                    c0, c1, c2, c3, c4, c5 = cols
                    self._data = [
                        {k0: c0[i], k1: c1[i], k2: c2[i], k3: c3[i], k4: c4[i], k5: c5[i]}
                        for i in range(n)
                    ]
                elif len(keys) == 5:
                    k0, k1, k2, k3, k4 = keys
                    c0, c1, c2, c3, c4 = cols
                    self._data = [
                        {k0: c0[i], k1: c1[i], k2: c2[i], k3: c3[i], k4: c4[i]}
                        for i in range(n)
                    ]
                elif len(keys) == 3:
                    k0, k1, k2 = keys
                    c0, c1, c2 = cols
                    self._data = [{k0: c0[i], k1: c1[i], k2: c2[i]} for i in range(n)]
                elif len(keys) == 2:
                    k0, k1 = keys
                    c0, c1 = cols
                    self._data = [{k0: c0[i], k1: c1[i]} for i in range(n)]
                elif len(keys) == 1:
                    k0 = keys[0]
                    c0 = cols[0]
                    self._data = [{k0: c0[i]} for i in range(n)]
                else:
                    self._data = [{k: d[k][i] for k in keys} for i in range(n)]
                return self._data
            self._ensure_arrow()
            if self._arrow_table is not None:
                show_id = bool(getattr(self, "_show_internal_id", False))
                if show_id:
                    self._data = self._arrow_table.to_pylist()
                else:
                    table = self._arrow_table
                    if '_id' in table.column_names:
                        table = table.drop(['_id'])
                    self._data = table.to_pylist()
        return self._data if self._data is not None else []
    
    def to_dict(self) -> List[dict]:
        """Convert results to a list of dictionaries.
        
        Returns:
            List[dict]: List of records as dictionaries, excluding the internal '_id' field.
        """
        return self._ensure_data()

    def tolist(self) -> List[dict]:
        """Convert results to a list of dictionaries.

        Alias for :meth:`to_dict`. Returns one dictionary per row, excluding
        the internal ``_id`` field.

        Returns:
            List[dict]: List of records as dictionaries.
        """
        return self._ensure_data()
    
    def to_pandas(self, zero_copy: bool = True):
        """Convert results to a pandas DataFrame.

        Args:
            zero_copy: If True, use ArrowDtype for zero-copy conversion (pandas 2.0+).
                If False, use traditional conversion copying data to NumPy.
                Defaults to True.

        Returns:
            pandas.DataFrame: DataFrame containing the query results.

        Raises:
            ImportError: If pandas is not available.

        Note:
            In zero-copy mode, DataFrame columns use Arrow native types (like string[pyarrow]).
            This performs better in most scenarios, but some NumPy operations may need
            type conversion first.
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas not available. Install with: pip install pandas")
        pd_mod = _ensure_pandas()

        def _pydict_to_dataframe(column_data: dict):
            # Normalize keys to plain str so pandas 3.x + pyarrow do not build
            # column indexes from ArrowStringArray after module reloads.
            normalized = {str(key): value for key, value in column_data.items()}
            columns = list(normalized.keys())
            return pd_mod.DataFrame(normalized, columns=columns)

        # Fast path: if we have lazy_pydict, convert directly to pandas without Arrow conversion
        if self._lazy_pydict is not None:
            show_id = bool(getattr(self, "_show_internal_id", False))
            d = self._lazy_pydict
            if show_id:
                return _pydict_to_dataframe(d)
            else:
                # Exclude _id column
                filtered = {k: v for k, v in d.items() if k != '_id'}
                return _pydict_to_dataframe(filtered)

        self._ensure_arrow()
        if self._arrow_table is not None:
            show_id = bool(getattr(self, "_show_internal_id", False))
            if zero_copy:
                # Zero-copy mode: use ArrowDtype (pandas 2.0+)
                try:
                    df = self._arrow_table.to_pandas(types_mapper=pd_mod.ArrowDtype)
                except (TypeError, AttributeError, AssertionError):
                    # Fallback for older pandas or pandas 3.x/pyarrow index incompatibilities
                    df = self._arrow_table.to_pandas()
            else:
                # Traditional mode: copy data to NumPy types
                df = self._arrow_table.to_pandas()

            if not show_id and '_id' in df.columns:
                df.set_index('_id', inplace=True)
                df.index.name = None
            return df
        
        # Fallback
        df = pd_mod.DataFrame(self._ensure_data())
        show_id = bool(getattr(self, "_show_internal_id", False))
        if not show_id and '_id' in df.columns:
            df.set_index('_id', inplace=True)
            df.index.name = None
        return df
    
    def to_polars(self):
        """Convert results to a polars DataFrame.
        
        Returns:
            polars.DataFrame: DataFrame containing the query results.
            
        Raises:
            ImportError: If polars is not available.
        """
        if not POLARS_AVAILABLE:
            raise ImportError("polars not available. Install with: pip install polars")
        pl_mod = _ensure_polars()
        
        if self._lazy_pydict is not None:
            show_id = bool(getattr(self, "_show_internal_id", False))
            d = self._lazy_pydict if show_id else {k: v for k, v in self._lazy_pydict.items() if k != '_id'}
            return pl_mod.DataFrame(d)

        self._ensure_arrow()
        if self._arrow_table is not None:
            df = pl_mod.from_arrow(self._arrow_table)
            show_id = bool(getattr(self, "_show_internal_id", False))
            if not show_id and '_id' in df.columns:
                df = df.drop('_id')
            return df
        return pl_mod.DataFrame(self._ensure_data())
    
    def to_arrow(self):
        """Convert results to a PyArrow Table.
        
        Returns:
            pyarrow.Table: Arrow Table containing the query results.
            
        Raises:
            ImportError: If pyarrow is not available.
        """
        if not ARROW_AVAILABLE:
            raise ImportError("pyarrow not available. Install with: pip install pyarrow")
        pa_mod = _ensure_pyarrow()
        
        self._ensure_arrow()
        if self._arrow_table is not None:
            show_id = bool(getattr(self, "_show_internal_id", False))
            if not show_id:
                # Remove _id column
                if '_id' in self._arrow_table.column_names:
                    return self._arrow_table.drop(['_id'])
            return self._arrow_table
        return pa_mod.Table.from_pylist(self._ensure_data())

    def to_record_batches(self, max_chunksize: int = None):
        """Return Arrow record batches without merging chunk payload buffers.

        Args:
            max_chunksize: Optional maximum rows per batch.

        Returns:
            List of ``pyarrow.RecordBatch`` objects preserving the result schema.
        """
        table = self.to_arrow()
        return table.to_batches(max_chunksize=max_chunksize)

    def iter_batches(self, max_chunksize: int = None):
        """Iterate Arrow record batches, suitable for bounded-memory consumers."""
        yield from self.to_record_batches(max_chunksize=max_chunksize)

    def to_lance(self, uri, mode: str = "create", **write_options):
        """Write results to a Lance dataset using the Arrow table path.

        This keeps the Python handoff Arrow-native; Lance still writes its own
        on-disk columnar format at the destination URI.
        """
        lance_mod = _ensure_lance()
        return lance_mod.write_dataset(
            self.to_arrow(),
            uri,
            mode=mode,
            **write_options,
        )
    
    @property
    def shape(self):
        self._ensure_arrow()
        if self._num_rows == 0:
            return (0, 0)
        if self._arrow_table is not None:
            return (self._arrow_table.num_rows, self._arrow_table.num_columns)
        if self._data:
            show_id = bool(getattr(self, "_show_internal_id", False))
            cols = list(self._data[0].keys())
            if not show_id and '_id' in cols:
                cols.remove('_id')
            return (len(self._data), len(cols))
        # When arrow_table is None (empty result), return (0, 0)
        return (0, 0)
    
    @property
    def columns(self):
        if self._lazy_pydict is not None:
            show_id = bool(getattr(self, "_show_internal_id", False))
            if show_id:
                return list(self._lazy_pydict)
            return [c for c in self._lazy_pydict if c != '_id']
        if self._arrow_table is not None:
            cols = self._arrow_table.column_names
            show_id = bool(getattr(self, "_show_internal_id", False))
            if show_id:
                return list(cols)
            return [c for c in cols if c != '_id']
        data = self._ensure_data()
        if not data:
            return []
        cols = list(data[0].keys())
        show_id = bool(getattr(self, "_show_internal_id", False))
        if not show_id and '_id' in cols:
            cols.remove('_id')
        return cols
    
    @property
    def ids(self):
        """[Deprecated] Please use get_ids() method"""
        return self.get_ids(return_list=True)
    
    def get_ids(self, return_list: bool = False):
        """Get the internal IDs of the result records.
        
        Args:
            return_list: If True, return as Python list.
                If False, return as numpy.ndarray (default, zero-copy, fastest).
                Defaults to False.
        
        Returns:
            numpy.ndarray or list: Array of record IDs.
        """
        self._ensure_arrow()
        if self._arrow_table is not None and '_id' in self._arrow_table.column_names:
            # Zero-copy path: directly convert from Arrow to numpy, bypassing Python objects
            id_array = self._arrow_table.column('_id').to_numpy()
            if return_list:
                return id_array.tolist()
            return id_array
        else:
            # Fallback: generate sequential IDs
            ids = np.arange(self._num_rows, dtype=np.uint64)
            if return_list:
                return ids.tolist()
            return ids

    def scalar(self):
        """Get single scalar value (for aggregate queries like COUNT(*))"""
        if self._lazy_pydict is not None:
            first_col = next((k for k in self._lazy_pydict if k != '_id'), None)
            if first_col and self._lazy_pydict[first_col]:
                return self._lazy_pydict[first_col][0]
            return None
        if self._arrow_table is not None and self._arrow_table.num_rows > 0:
            # Skip _id if present
            col_names = self._arrow_table.column_names
            col_idx = 0
            if col_names and col_names[0] == '_id' and len(col_names) > 1:
                col_idx = 1
            return self._arrow_table.column(col_idx)[0].as_py()

        data = self._ensure_data()
        if data:
            first_row = data[0]
            if first_row:
                first_key = next(iter(first_row.keys()))
                return first_row.get(first_key)
        return None

    def _row_at(self, idx: int) -> dict:
        """Materialize one row without expanding the entire result."""
        if idx < 0:
            idx += self._num_rows
        if idx < 0 or idx >= self._num_rows:
            raise IndexError("ResultView index out of range")
        if self._data is not None:
            return self._data[idx]

        show_id = bool(getattr(self, "_show_internal_id", False))
        if self._lazy_pydict is not None:
            return {
                key: values[idx]
                for key, values in self._lazy_pydict.items()
                if show_id or key != '_id'
            }

        self._ensure_arrow()
        if self._arrow_table is not None:
            table = self._arrow_table.slice(idx, 1)
            if not show_id and '_id' in table.column_names:
                table = table.drop(['_id'])
            rows = table.to_pylist()
            if rows:
                return rows[0]
        raise IndexError("ResultView index out of range")

    def first(self) -> Optional[dict]:
        """Get first row as dictionary (hide _id)"""
        return self._row_at(0) if self._num_rows else None
    
    def __len__(self):
        return self._num_rows
    
    def __iter__(self):
        return iter(self._ensure_data())
    
    def __getitem__(self, idx):
        if isinstance(idx, int):
            return self._row_at(idx)
        return self._ensure_data()[idx]
    
    def __repr__(self):
        return f"ResultView(rows={self._num_rows})"


def _empty_result_view() -> ResultView:
    # Create empty ResultView with no columns
    # Use a special marker to indicate truly empty result
    rv = ResultView(arrow_table=None, data=[])
    return rv


# Durability level type
DurabilityLevel = Literal['fast', 'safe', 'max']


def __getattr__(name: str):
    """Lazy-load ApexClient so `from apexbase._core import ApexStorage` does not
    pull pyarrow/pandas/polars into the process before the native extension is used."""
    if name == "ApexClient":
        from .client import ApexClient

        return ApexClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(list(globals().keys()) + ["ApexClient"])


# Exports
__all__ = ['ApexClient', 'ApexStorage', 'ResultView', 'DurabilityLevel', '__version__', 'FTS_AVAILABLE', 'ARROW_AVAILABLE', 'POLARS_AVAILABLE', 'PANDAS_AVAILABLE']
