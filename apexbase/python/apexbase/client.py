"""
ApexClient - High-performance embedded database client.

This module provides :class:`ApexClient`, which wraps ApexStorage with an
on-demand ``.apex`` storage engine, plus helpers for vector encoding and
optional auto query scheduling.
"""

from __future__ import annotations

import importlib.util
import os
import re
import threading
import queue
import contextlib
import ast

import json

from typing import List, Dict, Union, Optional, Tuple
from pathlib import Path
import numpy as np

from apexbase._core import ApexStorage
from . import ResultView, _empty_result_view, _registry, DurabilityLevel, _ensure_lance

pa = None
pd = None
pl = None
ARROW_AVAILABLE = importlib.util.find_spec("pyarrow") is not None
PANDAS_AVAILABLE = importlib.util.find_spec("pandas") is not None
POLARS_AVAILABLE = importlib.util.find_spec("polars") is not None


def _ensure_pyarrow():
    """
    Lazily import and cache the ``pyarrow`` module.

    Returns:
        The imported ``pyarrow`` module.

    Raises:
        ImportError: If pyarrow is not installed.
    """
    global pa, ARROW_AVAILABLE
    if pa is None:
        if not ARROW_AVAILABLE:
            raise ImportError("pyarrow not available. Install with: pip install pyarrow")
        import pyarrow as _pa
        pa = _pa
    return pa


def _ensure_pandas():
    """
    Lazily import and cache the ``pandas`` module.

    Returns:
        The imported ``pandas`` module.

    Raises:
        ImportError: If pandas is not installed.
    """
    global pd, PANDAS_AVAILABLE
    if pd is None:
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas not available. Install with: pip install pandas")
        import pandas as _pd
        pd = _pd
    return pd


def _ensure_polars():
    """
    Lazily import and cache the ``polars`` module.

    Returns:
        The imported ``polars`` module.

    Raises:
        ImportError: If polars is not installed.
    """
    global pl, POLARS_AVAILABLE
    if pl is None:
        if not POLARS_AVAILABLE:
            raise ImportError("polars not available. Install with: pip install polars")
        import polars as _pl
        pl = _pl
    return pl

import struct

# Null context manager for lock-free SELECT execution paths
_NULL_CONTEXT = contextlib.nullcontext()
_HOT_CACHE_MISS = object()

# ─────────────────────────────────────────────────────────────────────────────
# Auto Scheduler - Initialize scheduler lazily for parallel query execution
# ─────────────────────────────────────────────────────────────────────────────
_auto_scheduler_enabled = False
_auto_scheduler_initialized = False

def _init_auto_scheduler():
    """
    Initialize the Rust query scheduler if auto-scheduling is enabled.

    Creates a 4-worker scheduler on first successful call. Failures are
    ignored so clients can continue without parallel execution.

    Returns:
        None
    """
    global _auto_scheduler_initialized
    if not _auto_scheduler_initialized:
        try:
            from apexbase import _core
            _core.init_query_scheduler(4)
            _auto_scheduler_initialized = True
        except Exception:
            pass

def _enable_auto_scheduler():
    """
    Enable automatic concurrent query scheduling and initialize the pool.

    Returns:
        None
    """
    global _auto_scheduler_enabled
    _auto_scheduler_enabled = True
    _init_auto_scheduler()

def _disable_auto_scheduler():
    """
    Disable automatic concurrent query scheduling.

    Returns:
        None
    """
    global _auto_scheduler_enabled
    _auto_scheduler_enabled = False

# ─────────────────────────────────────────────────────────────────────────────
# Vector encoding / decoding helpers
# Vectors are stored as Binary columns: raw little-endian float32 bytes.
# ─────────────────────────────────────────────────────────────────────────────

def encode_vector(vec) -> bytes:
    """
    Encode a float vector to raw little-endian float32 bytes for storage.

    Accepts a list/tuple of numbers or a numpy array of any float/int dtype.

    Args:
        vec: Vector values as a sequence or numpy array.

    Returns:
        Packed little-endian float32 bytes suitable for a Binary column.

    Example::

        client.store([{"name": "item1", "vec": encode_vector([1.0, 2.0, 3.0])}])
    """
    if hasattr(vec, 'astype'):  # numpy array
        return vec.astype('<f4', copy=False).tobytes()
    return struct.pack(f'<{len(vec)}f', *[float(v) for v in vec])


def decode_vector(b: bytes) -> list:
    """
    Decode raw little-endian float32 bytes back to a Python list of floats.

    Args:
        b: Bytes previously produced by :func:`encode_vector` or equivalent
            Binary column storage.

    Returns:
        List of ``float`` values.

    Example::

        row = client.retrieve(1)
        floats = decode_vector(row["vec"])
    """
    return np.frombuffer(b, dtype='<f4', count=len(b) // 4).tolist()


def _is_vector_column(values) -> bool:
    """
    Return whether *values* looks like a column of float vectors.

    Args:
        values: Column values (list/iterable). Empty or all-None columns
            are treated as non-vector.

    Returns:
        ``True`` if the first non-None value is a 1-D numeric array or a
        list/tuple of numbers; otherwise ``False``.
    """
    if not values:
        return False
    # Find first non-None value
    first = next((v for v in values if v is not None), None)
    if first is None:
        return False
    # numpy array element (1-D)
    if hasattr(first, 'dtype') and hasattr(first, 'shape') and len(getattr(first, 'shape', ())) == 1:
        return True
    # plain list/tuple of numbers (not already bytes)
    if isinstance(first, (list, tuple)) and first and isinstance(first[0], (int, float)):
        return True
    return False


def _encode_vector_col(values) -> list:
    """
    Encode a column of vectors to a list of bytes objects.

    Args:
        values: Iterable of vector values (or ``None``).

    Returns:
        List of encoded bytes (or ``None`` for null entries).
    """
    return [None if v is None else encode_vector(v) for v in values]


# Pre-compiled regex for SQL validation (avoids re-compilation on every query)
_RE_CREATE_TABLE = re.compile(r"\bcreate\s+(table|view)\b", re.IGNORECASE)
_RE_FROM_TABLE = re.compile(r"\bfrom\s+([\w]+(?:\.[\w]+)?)", re.IGNORECASE)
_RE_FROM_OR_JOIN_TABLE = re.compile(r"\b(?:from|join)\s+([\w]+(?:\.[\w]+)?)", re.IGNORECASE)
_RE_QUALIFIED_REF = re.compile(r"\b\w+\.\w+\b")
_RE_SELECT_FROM = re.compile(r"\bselect\b(.*?)\bfrom\b", re.IGNORECASE | re.DOTALL)
_RE_AGGREGATE_FUNC = re.compile(r"\b(count|sum|avg|min|max)\s*\(", re.IGNORECASE)
_RE_EXPLICIT_ID = re.compile(r"(^|[^\w])(_id|\"_id\")([^\w]|$)|\._id([^\w]|$)", re.IGNORECASE)
_RE_POINT_LOOKUP_ID = re.compile(r"\bwhere\s+_id\s*=\s*(\d+)\b", re.IGNORECASE)
_RE_SIMPLE_COUNT_STAR = re.compile(
    r"^\s*select\s+count\s*\(\s*\*\s*\)(?:\s+(?:as\s+)?([A-Za-z_][\w]*))?\s+from\s+([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)\s*;?\s*$",
    re.IGNORECASE,
)
_RE_SIMPLE_POINT_LOOKUP = re.compile(
    r"^\s*select\s+\*\s+from\s+([A-Za-z_][\w]*)\s+where\s+_id\s*=\s*(\d+)\s*;?\s*$",
    re.IGNORECASE,
)
_RE_SIMPLE_PROJECTED_POINT_LOOKUP = re.compile(
    r"^\s*select\s+(.+?)\s+from\s+([A-Za-z_][\w]*)\s+where\s+_id\s*=\s*(\d+)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_RE_SIMPLE_SCAN_SHAPE = re.compile(
    r"^\s*select\s+(.+?)\s+from\s+([A-Za-z_][\w]*)(?:\s+limit\s+(\d+)(?:\s+offset\s+(\d+))?)?\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_RE_SIMPLE_SELECT_FROM = re.compile(r"^\s*select\s+(.*?)\s+from\s+", re.IGNORECASE | re.DOTALL)
_RE_SIMPLE_FROM_TABLE = re.compile(r"\bfrom\s+([A-Za-z_][\w]*)\b", re.IGNORECASE)
_RE_SIMPLE_ID_IN = re.compile(r"\bwhere\s+_id\s+in\s*\(([^)]*)\)\s*;?\s*$", re.IGNORECASE)
_RE_SIMPLE_STRING_EQ = re.compile(
    r"\bwhere\s+([A-Za-z_][\w]*)\s*=\s*'([^']*)'\s*;?\s*$",
    re.IGNORECASE,
)
_RE_SIMPLE_STRING_EQ_LIMIT = re.compile(
    r"\bwhere\s+([A-Za-z_][\w]*)\s*=\s*'([^']*)'\s+limit\s+(\d+)(?:\s+offset\s+(\d+))?\s*;?\s*$",
    re.IGNORECASE,
)
_RE_SIMPLE_NUMERIC_RANGE_LIMIT = re.compile(
    r"^\s*select\s+\*\s+from\s+([A-Za-z_][\w]*)\s+where\s+([A-Za-z_][\w]*)\s*(=|>=|>|<=|<)\s*(-?\d+(?:\.\d+)?)\s+limit\s+(\d+)(?:\s+offset\s+(\d+))?\s*;?\s*$",
    re.IGNORECASE,
)
_RE_SIMPLE_NUMERIC_FILTERED_AGG = re.compile(
    r"^\s*select\s+(.+?)\s+from\s+([A-Za-z_][\w]*)\s+where\s+([A-Za-z_][\w]*)\s*(=|>=|>|<=|<)\s*(-?\d+(?:\.\d+)?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_RE_SIMPLE_STRING_FILTERED_AGG = re.compile(
    r"^\s*select\s+(.+?)\s+from\s+([A-Za-z_][\w]*)\s+where\s+([A-Za-z_][\w]*)\s*=\s*'([^']*)'\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_RE_SIMPLE_GROUP_VIEW_OUTER = re.compile(
    r"^\s*select\s+(.+?)\s+from\s+([A-Za-z_][\w]*)\s+where\s+([A-Za-z_][\w]*)\s*(>=|>)\s*([+-]?\d+(?:\.\d+)?)\s+order\s+by\s+([A-Za-z_][\w]*)\s+desc\s+limit\s+(\d+)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_RE_SIMPLE_NUMERIC_UPDATE_BY_ID = re.compile(
    r"^\s*update\s+([A-Za-z_][\w]*)\s+set\s+([A-Za-z_][\w]*)\s*=\s*(-?\d+(?:\.\d+)?)\s+where\s+_id\s*=\s*(\d+)\s*;?\s*$",
    re.IGNORECASE,
)
_RE_SIMPLE_INSERT_VALUES = re.compile(
    r"^\s*insert\s+into\s+([A-Za-z_][\w]*)\s*\(([^)]*)\)\s*values\s*(.+?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_RE_BOUND_TOPK = re.compile(
    r"""^\s*select\s+explode_rename\(\s*topk_distance\(\s*([A-Za-z_][\w]*)\s*,\s*\?\s*,\s*(\d+)\s*,\s*'([^']+)'\s*\)\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*\)\s+from\s+([A-Za-z_][\w]*)\s*;?\s*$""",
    re.IGNORECASE,
)


def _projection_columns_from_text(projection: str) -> Optional[List[str]]:
    """
    Parse a simple SELECT projection list into plain column names.

    Only accepts comma-separated identifiers (optionally table-qualified).
    Expressions, aliases, wildcards, and aggregates return ``None``.

    Args:
        projection: Text between ``SELECT`` and ``FROM``.

    Returns:
        Ordered list of column names, or ``None`` if not a simple projection.
    """
    if not projection or projection == "*":
        return None

    columns = []
    seen = set()
    for raw in projection.split(','):
        part = raw.strip()
        part_upper = part.upper()
        if (not part or part == "*" or part.endswith(".*")
                or any(ch in part for ch in ("(", ")", "+", "-", "/", "'"))
                or " AS " in part_upper
                or any(ch.isspace() for ch in part)):
            return None
        name = part.rsplit('.', 1)[-1].strip('"`')
        if not name or name == "*" or name in seen:
            return None
        seen.add(name)
        columns.append(name)
    return columns or None


def _simple_projection_columns(sql: str) -> Optional[List[str]]:
    """
    Return plain SELECT columns for simple projected SQL fast paths.

    Args:
        sql: SQL statement text.

    Returns:
        List of column names, or ``None`` when the statement is not a
        simple projected SELECT.
    """
    m = _RE_SIMPLE_SELECT_FROM.match(sql)
    if not m:
        return None
    return _projection_columns_from_text(m.group(1).strip())


def _simple_from_table(sql: str) -> Optional[str]:
    """
    Extract the first simple ``FROM`` table identifier from SQL.

    Args:
        sql: SQL statement text.

    Returns:
        Table name string, or ``None`` if no match.
    """
    m = _RE_SIMPLE_FROM_TABLE.search(sql)
    return m.group(1) if m else None


def _simple_id_list(sql: str) -> Optional[List[int]]:
    """
    Parse a simple ``WHERE _id IN (...)`` integer list from SQL.

    Args:
        sql: SQL statement text.

    Returns:
        List of integer IDs, or ``None`` if the clause is missing/invalid.
    """
    m = _RE_SIMPLE_ID_IN.search(sql)
    if not m:
        return None
    ids = []
    for part in m.group(1).split(','):
        part = part.strip()
        if not part or not part.isdigit():
            return None
        ids.append(int(part))
    return ids


def _classify_sql_route(sql: str, sql_upper: Optional[str] = None):
    """
    Classify one statement for Python-side locking and fast-path routing.

    Args:
        sql: Original SQL text.
        sql_upper: Optional precomputed upper-cased stripped SQL. Computed
            from *sql* when omitted.

    Returns:
        Tuple ``(sig, count_star_match, simple_projection)`` where:

        - ``sig`` is a route signature string (e.g. ``point_lookup``,
          ``write``, ``complex``).
        - ``count_star_match`` is a regex match for ``COUNT(*)`` queries,
          otherwise ``None``.
        - ``simple_projection`` is a list of projected columns or ``None``.
    """
    if sql_upper is None:
        sql_upper = sql.strip().upper()

    trimmed = sql.strip().rstrip(';').strip()
    count_star_match = None
    simple_projection = _simple_projection_columns(sql)

    if ';' in trimmed:
        sig = 'multi'
    elif (count_star_match := _RE_SIMPLE_COUNT_STAR.match(sql)):
        sig = 'count_star'
    elif (simple_projection
            and sql_upper.startswith('SELECT')
            and ('WHERE _ID =' in sql_upper or 'WHERE _ID=' in sql_upper)
            and 'LIMIT' not in sql_upper and 'ORDER' not in sql_upper
            and 'GROUP' not in sql_upper and 'JOIN' not in sql_upper
            and ' AND ' not in sql_upper and ' OR ' not in sql_upper
            and ' NOT ' not in sql_upper and ' IN ' not in sql_upper
            and ';' not in sql_upper):
        sig = 'projected_point_lookup'
    elif (simple_projection
            and sql_upper.startswith('SELECT')
            and _RE_SIMPLE_ID_IN.search(sql)
            and 'LIMIT' not in sql_upper and 'ORDER' not in sql_upper
            and 'GROUP' not in sql_upper and 'JOIN' not in sql_upper
            and ' AND ' not in sql_upper and ' OR ' not in sql_upper
            and ' NOT ' not in sql_upper
            and ';' not in sql_upper):
        sig = 'projected_batch_lookup'
    elif (simple_projection
            and sql_upper.startswith('SELECT')
            and 'WHERE' not in sql_upper and 'LIMIT' not in sql_upper
            and 'ORDER' not in sql_upper and 'GROUP' not in sql_upper
            and 'JOIN' not in sql_upper and 'DISTINCT' not in sql_upper
            and ';' not in sql_upper):
        sig = 'projected_full_scan'
    elif (simple_projection
            and sql_upper.startswith('SELECT')
            and 'LIMIT' in sql_upper
            and 'WHERE' not in sql_upper and 'ORDER' not in sql_upper
            and 'GROUP' not in sql_upper and 'JOIN' not in sql_upper):
        sig = 'projected_scan_limit'
    elif (simple_projection
            and sql_upper.startswith('SELECT')
            and 'WHERE' in sql_upper
            and _RE_SIMPLE_STRING_EQ.search(sql)
            and 'LIMIT' not in sql_upper and 'ORDER' not in sql_upper
            and 'GROUP' not in sql_upper and 'JOIN' not in sql_upper
            and 'BETWEEN' not in sql_upper and ' IN ' not in sql_upper
            and ' LIKE ' not in sql_upper
            and ' AND ' not in sql_upper and ' OR ' not in sql_upper
            and '>' not in sql_upper and '<' not in sql_upper):
        sig = 'projected_string_filter'
    elif (simple_projection
            and sql_upper.startswith('SELECT')
            and 'WHERE' in sql_upper
            and _RE_SIMPLE_STRING_EQ_LIMIT.search(sql)
            and 'ORDER' not in sql_upper
            and 'GROUP' not in sql_upper and 'JOIN' not in sql_upper
            and 'BETWEEN' not in sql_upper and ' IN ' not in sql_upper
            and ' LIKE ' not in sql_upper
            and ' AND ' not in sql_upper and ' OR ' not in sql_upper
            and '>' not in sql_upper and '<' not in sql_upper):
        sig = 'projected_string_filter_limit'
    elif (sql_upper.startswith('SELECT *')
            and 'WHERE' in sql_upper
            and _RE_SIMPLE_STRING_EQ_LIMIT.search(sql)
            and 'ORDER' not in sql_upper
            and 'GROUP' not in sql_upper and 'JOIN' not in sql_upper
            and 'BETWEEN' not in sql_upper and ' IN ' not in sql_upper
            and ' LIKE ' not in sql_upper
            and ' AND ' not in sql_upper and ' OR ' not in sql_upper
            and '>' not in sql_upper and '<' not in sql_upper):
        sig = 'string_filter_limit'
    elif (sql_upper.startswith('SELECT *')
            and ('WHERE _ID =' in sql_upper or 'WHERE _ID=' in sql_upper)
            and 'LIMIT' not in sql_upper and 'ORDER' not in sql_upper
            and 'GROUP' not in sql_upper and 'JOIN' not in sql_upper
            and ' AND ' not in sql_upper and ' OR ' not in sql_upper
            and ' NOT ' not in sql_upper and ' IN ' not in sql_upper
            and ';' not in sql_upper):
        sig = 'point_lookup'
    elif (sql_upper.startswith('SELECT *')
            and 'WHERE _ID IN' in sql_upper
            and 'LIMIT' not in sql_upper and 'ORDER' not in sql_upper
            and 'GROUP' not in sql_upper and 'JOIN' not in sql_upper
            and ' AND ' not in sql_upper and ' OR ' not in sql_upper
            and ' NOT ' not in sql_upper
            and ';' not in sql_upper):
        sig = 'batch_lookup'
    elif (sql_upper.startswith('SELECT *') and 'LIMIT' in sql_upper
            and 'WHERE' not in sql_upper and 'ORDER' not in sql_upper
            and 'GROUP' not in sql_upper and 'JOIN' not in sql_upper):
        sig = 'scan_limit'
    elif (sql_upper.startswith('SELECT')
            and ('FROM READ_CSV(' in sql_upper
                 or 'FROM READ_PARQUET(' in sql_upper
                 or 'FROM READ_JSON(' in sql_upper)):
        sig = 'table_func'
    elif (sql_upper.startswith('BEGIN')
          or sql_upper in ('COMMIT', 'COMMIT;', 'ROLLBACK', 'ROLLBACK;')
          or sql_upper.startswith('SAVEPOINT')
          or sql_upper.startswith('RELEASE')
          or sql_upper.startswith('ROLLBACK TO')):
        sig = 'transaction'
    elif sql_upper.startswith(('INSERT', 'DELETE', 'UPDATE', 'TRUNCATE',
                               'ALTER', 'DROP', 'CREATE', 'COPY')):
        sig = 'write'
    elif sql_upper.startswith(('SET ', 'RESET ')):
        sig = 'session'
    elif (sql_upper.startswith('SELECT *') and ' LIKE ' in sql_upper
            and 'WHERE' in sql_upper and 'NOT LIKE' not in sql_upper
            and 'LIMIT' not in sql_upper and 'ORDER' not in sql_upper
            and 'GROUP' not in sql_upper and 'JOIN' not in sql_upper
            and ' AND ' not in sql_upper and ' OR ' not in sql_upper
            and "'" in sql):
        sig = 'like'
    else:
        sig = 'complex'

    return sig, count_star_match, simple_projection


def _sql_route_family(sig: str) -> str:
    """
    Collapse a detailed Python fast-path signature into a routing family.

    Args:
        sig: Signature from :func:`_classify_sql_route`.

    Returns:
        One of ``'write'``, ``'transaction'``, ``'multi'``, ``'session'``,
        or ``'read'``.
    """
    if sig == 'write':
        return 'write'
    if sig in ('transaction', 'multi', 'session'):
        return sig
    return 'read'


def _split_simple_insert_value_groups(values_text: str) -> Optional[List[str]]:
    """
    Split an INSERT ``VALUES`` clause into parenthesized value groups.

    Respects nested parentheses and single-quoted string literals
    (including escaped ``''``).

    Args:
        values_text: Text after the ``VALUES`` keyword.

    Returns:
        List of group strings such as ``'(1, \'a\')'``, or ``None`` if
        the text is malformed.
    """
    groups = []
    start = None
    depth = 0
    in_quote = False
    i = 0
    while i < len(values_text):
        ch = values_text[i]
        if ch == "'":
            if in_quote and i + 1 < len(values_text) and values_text[i + 1] == "'":
                i += 2
                continue
            in_quote = not in_quote
        elif not in_quote:
            if ch == "(":
                if depth == 0:
                    start = i
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth < 0:
                    return None
                if depth == 0 and start is not None:
                    groups.append(values_text[start:i + 1])
                    start = None
            elif depth == 0 and ch not in ", \t\r\n":
                return None
        i += 1
    if in_quote or depth != 0:
        return None
    return groups or None


def _parse_simple_insert_values(sql: str):
    """
    Parse a simple ``INSERT INTO t (cols) VALUES (...)`` statement.

    Args:
        sql: SQL statement text.

    Returns:
        ``(table_name, rows)`` where *rows* is a list of dicts mapping
        column names to literal values; or ``None`` if the statement is
        not a supported simple insert (e.g. includes ``_id`` or non-literals).
    """
    match = _RE_SIMPLE_INSERT_VALUES.match(sql)
    if not match:
        return None
    table = match.group(1)
    columns = [c.strip().strip('"`') for c in match.group(2).split(",")]
    if not columns or any(not c or c == "_id" for c in columns):
        return None
    groups = _split_simple_insert_value_groups(match.group(3))
    if not groups:
        return None
    rows = []
    for group in groups:
        try:
            values = ast.literal_eval(group)
        except Exception:
            return None
        if not isinstance(values, tuple):
            values = (values,)
        if len(values) != len(columns):
            return None
        rows.append(dict(zip(columns, values)))
    return table, rows


class ApexClient:
    """
    High-performance embedded database client for ApexBase.

    Wraps :class:`~apexbase._core.ApexStorage` with on-demand ``.apex`` storage,
    multi-database/table management, SQL execution, vector helpers, BLOB APIs,
    and optional full-text search (FTS).

    Typical usage::

        with ApexClient("./data") as client:
            client.create_table("users", {"name": "string", "age": "int64"})
            client.store({"name": "Ada", "age": 36})
            rows = client.execute("SELECT * FROM users WHERE age > 30").to_dict()

    Attributes:
        current_database: Name of the active database (``'default'`` = root).
        current_table: Name of the active table, or ``None`` if unset.
    """
    
    def __init__(
        self, 
        dirpath=None, 
        batch_size: int = 1000, 
        drop_if_exists: bool = False,
        enable_cache: bool = True,
        cache_size: int = 10000,
        prefer_arrow_format: bool = True,
        durability: DurabilityLevel = 'fast',
        _auto_manage: bool = True
    ):
        """
        Create an ApexClient bound to a directory-backed ``.apex`` database.

        Args:
            dirpath: Database directory path. Defaults to the current directory.
            batch_size: Hint for batch-oriented write paths (default ``1000``).
            drop_if_exists: If ``True``, recreate storage and clear persisted
                FTS config under this directory.
            enable_cache: Whether client-side caches are enabled (default ``True``).
            cache_size: Soft cache size hint (default ``10000``).
            prefer_arrow_format: Prefer Arrow-friendly result materialization
                when pyarrow is available (default ``True``).
            durability: Write durability level: ``'fast'``, ``'safe'``, or
                ``'max'`` (default ``'fast'``).
            _auto_manage: Internal flag. When ``True`` (default), register with
                the process-wide client/storage registry for sharing and cleanup.

        Returns:
            None

        Raises:
            ValueError: If *durability* is not a recognised level.
        """
        if dirpath is None:
            dirpath = "."
        
        self._dirpath = Path(dirpath)
        self._dirpath.mkdir(parents=True, exist_ok=True)
        
        # Use .apex file format for V3 storage
        self._db_path = self._dirpath / "apexbase.apex"
        self._auto_manage = _auto_manage
        self._is_closed = False
        self._shared_storage = None  # Will be set by registry if sharing
        self._is_shared_client = False  # True if using shared storage
        
        # Register to global registry (this may set _shared_storage for sharing)
        if self._auto_manage:
            _registry.register(self, str(self._db_path))
        self._storage_lock = None
        if self._auto_manage:
            self._storage_lock = _registry.get_storage_lock(str(self._db_path))
        
        # Validate durability parameter
        if durability not in ('fast', 'safe', 'max'):
            raise ValueError(f"durability must be 'fast', 'safe', or 'max', got '{durability}'")
        self._durability = durability
        
        # Initialize storage: use shared if available, otherwise create new.
        # The per-path lock serializes concurrent constructors for the same DB.
        storage_context = self._storage_lock if self._storage_lock is not None else _NULL_CONTEXT
        with storage_context:
            shared_storage = self._shared_storage
            if shared_storage is None and self._auto_manage and not drop_if_exists:
                shared_storage = _registry.get_storage(str(self._db_path))
                if shared_storage is not None:
                    self._is_shared_client = True

            # When drop_if_exists=True, always create fresh storage (ignore shared)
            if shared_storage is not None and not drop_if_exists:
                self._storage = shared_storage
            else:
                try:
                    self._storage = ApexStorage(
                        str(self._db_path),
                        drop_if_exists=drop_if_exists,
                        durability=durability,
                    )
                except TypeError:
                    self._storage = ApexStorage(str(self._db_path), drop_if_exists=drop_if_exists)
                self._is_shared_client = False
                if self._auto_manage:
                    _registry.set_storage(str(self._db_path), self._storage)
        
        self._connected = True
        self._lock = threading.RLock()
        
        self._current_table = None  # No default table - user must create/use a table explicitly
        self._current_database = 'default'  # Active database name ('default' = root dir)
        self._batch_size = batch_size
        self._enable_cache = enable_cache
        self._cache_size = cache_size
        
        # FTS configuration
        self._fts_tables: Dict[str, Dict] = {}
        self._fts_initialized_tables = set()
        self._fts_dirty: bool = False

        # Persisted FTS configuration path
        self._fts_config_path = self._dirpath / "fts_config.json"
        self._fts_config_known_present = False
        self._fts_config_mtime_ns = None
        self._view_catalog_path = self._dirpath / ".apex_views.json"
        self._view_catalog_known_present = False
        self._view_catalog_mtime_ns = None
        self._view_catalog_views = {}

        # If recreating DB, clear any persisted FTS config
        if drop_if_exists:
            try:
                if self._fts_config_path.exists():
                    self._fts_config_path.unlink()
            except Exception:
                pass

        # Load persisted FTS config (if any)
        self._load_fts_config()
        
        self._prefer_arrow_format = prefer_arrow_format and ARROW_AVAILABLE
        self._registry = _registry
        self._has_writes = False  # True after any write; disables _storage.execute() fast paths
        self._last_exact_replace_key = None
        self._last_exact_replace_data = None
        self._last_exact_numeric_update = None
        self._last_exact_numeric_update_result = None
        self._last_missing_delete_key = None
        self._simple_sql_cache = {}
        self._numeric_range_rows_cache = {}
        self._schemaless_tables = set()
        self._buffered_writes_enabled = False
        self._buffered_write_rows = []
        self._buffered_write_table = None
        self._buffered_write_flush_rows = 0
        self._in_txn = False
        self._fast_txn_active = False
        self._fast_txn_read_only = False
        self._fast_txn_writes = []
        self._memtable_single_writes_enabled = (
            durability == 'fast'
            and os.environ.get("APEXBASE_DISABLE_MEMTABLE_SINGLE_WRITE") != "1"
        )
        self._experimental_delta_single_writes_enabled = (
            os.environ.get("APEXBASE_EXPERIMENTAL_DELTA_SINGLE_WRITE") == "1"
        )
        self._experimental_memtable_single_writes_enabled = (
            os.environ.get("APEXBASE_EXPERIMENTAL_MEMTABLE_SINGLE_WRITE") == "1"
        )
        self._store_one = getattr(self._storage, "store_one", None)
        self._store_one_memtable = getattr(self._storage, "store_one_memtable", None)
        self._store_one_delta = getattr(self._storage, "store_one_delta", None)
        self._store_one_delta_durable = getattr(self._storage, "store_one_delta_durable", None)

    def _load_fts_config(self) -> None:
        """
        Load persisted FTS table configuration from ``fts_config.json``.

        Skips reload when the file mtime is unchanged. Corrupt or missing
        files reset in-memory FTS state.

        Returns:
            None
        """
        try:
            try:
                stat = self._fts_config_path.stat()
            except FileNotFoundError:
                self._fts_config_known_present = False
                self._fts_config_mtime_ns = None
                self._fts_initialized_tables.clear()
                if self._fts_tables:
                    self._fts_tables = {}
                return
            mtime_ns = stat.st_mtime_ns
            if self._fts_config_known_present and self._fts_config_mtime_ns == mtime_ns:
                return
            with open(self._fts_config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                # Only accept dict[str, dict] shape
                self._fts_tables = {str(k): v for k, v in data.items() if isinstance(v, dict)}
                self._fts_initialized_tables.clear()
            self._fts_config_known_present = True
            self._fts_config_mtime_ns = mtime_ns
        except Exception:
            # Best-effort: if config is corrupted, ignore it
            self._fts_tables = {}
            self._fts_initialized_tables.clear()
            self._fts_config_known_present = False
            self._fts_config_mtime_ns = None

    def _save_fts_config(self) -> None:
        """
        Persist current FTS table configuration to ``fts_config.json``.

        Writes via a temporary file and atomic replace. Failures are ignored.

        Returns:
            None
        """
        try:
            temp_path = self._fts_config_path.with_suffix('.json.tmp')
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(self._fts_tables, f, ensure_ascii=False)
                f.flush()
            os.replace(temp_path, self._fts_config_path)
            try:
                self._fts_config_mtime_ns = self._fts_config_path.stat().st_mtime_ns
            except FileNotFoundError:
                self._fts_config_mtime_ns = None
            self._fts_config_known_present = True
        except Exception:
            pass

    def _is_fts_enabled(self, table_name: str = None) -> bool:
        """
        Return whether full-text search is enabled for a table.

        Args:
            table_name: Table to check. Defaults to the current table.

        Returns:
            ``True`` if FTS is marked enabled in the local config.
        """
        table = table_name or self._current_table
        return table in self._fts_tables and self._fts_tables[table].get('enabled', False)
    
    def _get_fts_config(self, table_name: str = None) -> Optional[Dict]:
        """
        Return the FTS configuration dict for a table.

        Args:
            table_name: Table to look up. Defaults to the current table.

        Returns:
            Config dict, or ``None`` if the table has no FTS entry.
        """
        table = table_name or self._current_table
        return self._fts_tables.get(table)
    
    def _ensure_fts_initialized(self, table_name: str = None) -> bool:
        """
        Lazily initialize the Rust FTS engine for a table on first use.

        Args:
            table_name: Table to initialize. Defaults to the current table.
                Initialization only proceeds when the table is currently selected.

        Returns:
            ``True`` if FTS is enabled and the engine is ready; otherwise ``False``.
        """
        table = table_name or self._current_table
        if not self._is_fts_enabled(table):
            return False
        if table in self._fts_initialized_tables:
            return True

        # Lazily initialize Rust FTS engine on first use, using persisted config
        try:
            if table != self._current_table:
                return False
            fts_config = self._fts_tables.get(table, {})
            cfg = fts_config.get('config', {}) if isinstance(fts_config, dict) else {}
            index_fields = fts_config.get('index_fields') if isinstance(fts_config, dict) else None
            self._storage._init_fts(
                index_fields=index_fields,
                lazy_load=bool(cfg.get('lazy_load', False)),
                cache_size=int(cfg.get('cache_size', 10000)),
            )
            self._fts_initialized_tables.add(table)
        except Exception:
            # If initialization fails, report as not initialized
            return False

        return True

    @contextlib.contextmanager
    def _fts_table_context(self, table: str):
        """
        Temporarily select *table* for FTS operations, then restore the previous table.

        Args:
            table: Target table name for the duration of the context.

        Yields:
            None

        Returns:
            A context manager.
        """
        original = self._current_table
        if table != original:
            self.use_table(table)
        try:
            yield
        finally:
            if original is not None and table != original:
                self.use_table(original)

    def _sync_fts_config_from_disk(self, initialize_current: bool = True) -> None:
        """
        Refresh Python-side FTS state after SQL DDL updates ``fts_config.json``.

        Args:
            initialize_current: If ``True`` (default), also initialize the FTS
                engine for the currently selected table when enabled.

        Returns:
            None
        """
        self._load_fts_config()
        if initialize_current and self._current_table and self._is_fts_enabled(self._current_table):
            self._ensure_fts_initialized(self._current_table)
    
    def _check_connection(self):
        """
        Raise if this client has been closed or has no storage handle.

        Returns:
            None

        Raises:
            RuntimeError: When the connection is closed.
        """
        if self._is_closed or self._storage is None:
            raise RuntimeError("ApexClient connection has been closed, cannot perform operations.")

    def _invalidate_replace_cache(self) -> None:
        """
        Clear exact-replace / numeric-update / missing-delete and numeric-range caches.

        Returns:
            None
        """
        self._last_exact_replace_key = None
        self._last_exact_replace_data = None
        self._last_exact_numeric_update = None
        self._last_exact_numeric_update_result = None
        self._last_missing_delete_key = None
        cache = getattr(self, '_numeric_range_rows_cache', None)
        if cache is not None:
            cache.clear()

    def _numeric_range_cache_token(self):
        """
        Build a cache token for numeric-range query result reuse.

        Returns:
            Opaque token derived from table generation state, used to validate
            cached numeric-range rows; or ``None`` when caching is disabled,
            a transaction is active, or overlay writes are pending.
        """
        if (not self._enable_cache or not self._current_table
                or getattr(self, '_in_txn', False)
                or getattr(self, '_fast_txn_active', False)):
            return None
        try:
            if self._storage.has_pending_overlay_writes():
                return None
            return self._storage._table_epoch()
        except (AttributeError, RuntimeError):
            return None

    def _pending_memtable_point_miss(self) -> bool:
        """Return whether a point-read miss is authoritative for a pending memtable."""
        try:
            return bool(
                self._storage.has_pending_overlay_writes()
                and self._storage.has_pending_memtable_rows()
            )
        except (AttributeError, RuntimeError):
            return False

    def _recover_projected_point_row(self, point_id: int, columns) -> Optional[dict]:
        """Recover a projected point miss through the visibility-aware full-row path."""
        try:
            row = self._storage.retrieve(point_id)
        except (AttributeError, RuntimeError):
            return None
        if row is None or any(column not in row for column in columns):
            return None
        return {column: row[column] for column in columns}

    def _remember_exact_replace(self, id_: int, data: dict) -> None:
        """
        Cache the last successful exact ``replace`` payload for idempotent repeats.

        Args:
            id_: Row ``_id`` that was replaced.
            data: Replacement row dict.

        Returns:
            None
        """
        self._last_exact_replace_key = (self._current_database, self._current_table, int(id_))
        self._last_exact_replace_data = dict(data)

    def _remember_exact_numeric_update(self, row_id: int, column: str, value, updated=True) -> None:
        """
        Cache the last exact numeric ``UPDATE ... WHERE _id = ...`` result.

        Args:
            row_id: Updated row ``_id``.
            column: Updated column name.
            value: New numeric value.
            updated: Whether the update reported success (default ``True``).

        Returns:
            None
        """
        self._last_exact_numeric_update = (
            self._current_database,
            self._current_table,
            int(row_id),
            str(column),
            value,
        )
        self._last_exact_numeric_update_result = updated

    def _result_view_from_columns_dict(
        self,
        sql: str,
        columns_dict,
        show_internal_id: bool,
    ) -> 'ResultView':
        """
        Build a :class:`ResultView` from a columnar ``columns_dict`` payload.

        Args:
            sql: Original SQL (reserved for callers / debugging context).
            columns_dict: Mapping of column name to value lists.
            show_internal_id: Whether ``_id`` should be visible on the view.

        Returns:
            A :class:`ResultView` wrapping the columnar data.
        """
        rv = ResultView(lazy_pydict=columns_dict)
        rv._show_internal_id = show_internal_id
        return rv
    
    def _ensure_table_selected(self):
        """
        Ensure a current table is selected before table-scoped operations.

        Returns:
            None

        Raises:
            RuntimeError: If no table is currently selected. Call
                :meth:`create_table` or :meth:`use_table` first.
        """
        if self._current_table is None:
            raise RuntimeError("No table selected. Call create_table() or use_table() first.")

    # ============ Database Management ============

    def use_database(self, database: str = 'default') -> 'ApexClient':
        """Switch to a named database. Creates it if it doesn't exist.

        'default' (or '') means the root directory — backward-compatible behaviour.
        Named databases (e.g. 'analytics') are stored in sub-directories of the
        root directory and each has its own isolated set of tables.

        Args:
            database: Database name. Use 'default' for the root-level tables.

        Returns:
            self (for method chaining)
        """
        self._check_connection()
        with self._lock:
            self._flush_pending_memtable_rows_for_read()
            self.flush_buffered_writes()
            self._storage.use_database_(database)
            self._current_database = database if database else 'default'
            self._current_table = None
            self._invalidate_replace_cache()
        return self

    def use(self, database: str = 'default', table: str = None) -> 'ApexClient':
        """Switch to a database and optionally select a table within it.

        Combines use_database() + use_table() / create_table() in one call.
        If the table does not exist in the target database it will be created.

        Args:
            database: Database name (default = root-level).
            table: Table name to switch to. If None only the database is switched.

        Returns:
            self (for method chaining)
        """
        self.use_database(database)
        if table is not None:
            with self._lock:
                try:
                    self.use_table(table)
                except Exception:
                    self.create_table(table)
        return self

    @property
    def current_database(self) -> str:
        """
        Return the currently active database name.

        Returns:
            Active database name (``'default'`` for root-level tables).
        """
        self._check_connection()
        return self._current_database

    def list_databases(self) -> list:
        """
        List all available databases.

        ``'default'`` is always included (root-level tables). Other entries are
        named sub-directories inside the root directory.

        Returns:
            Sorted list of database name strings.
        """
        self._check_connection()
        return self._storage.list_databases_()

    # ============ Table Management ============

    def use_table(self, table_name: str):
        """
        Switch the active table used by subsequent read/write operations.

        Flushes pending memtable/buffered writes when switching away from another
        table, and refreshes FTS config when needed.

        Args:
            table_name: Name of the table to select.

        Returns:
            None

        Raises:
            RuntimeError: If the client is closed.
            Exception: Propagated from the storage layer if the table is missing.
        """
        self._check_connection()
        storage_lock = getattr(self, '_storage_lock', None)
        storage_context = storage_lock if storage_lock is not None else _NULL_CONTEXT
        with storage_context:
            with self._lock:
                switching_tables = (
                    self._current_table is not None
                    and self._current_table != table_name
                )
                if switching_tables:
                    self._flush_pending_memtable_rows_for_read()
                    self.flush_buffered_writes()
                self._storage.use_table(table_name)
                if self._current_table != table_name:
                    self._invalidate_replace_cache()
        self._current_table = table_name
        if self._fts_config_known_present or self._fts_tables:
            self._sync_fts_config_from_disk()

    @property
    def current_table(self) -> str:
        """
        Return the currently selected table name.

        Returns:
            Current table name, or ``None`` if no table is selected.
        """
        self._check_connection()
        return self._current_table

    def create_table(self, table_name: str, schema: dict = None):
        """
        Create a new table and select it as the current table.

        Args:
            table_name: Name of the table to create.
            schema: Optional dict mapping column names to type strings.
                Pre-defining schema avoids type inference on the first insert.
                Supported types include: ``int8``/``int16``/``int32``/``int64``,
                ``uint8``/``uint16``/``uint32``/``uint64``, ``float32``/``float64``,
                ``bool``, ``string``, ``binary``, ``blob``.
                Example: ``{"name": "string", "age": "int64"}``.

        Returns:
            None

        Raises:
            ValueError: If table creation fails (storage ``OSError`` wrapped).
            RuntimeError: If the client is closed.
        """
        self._check_connection()
        storage_lock = getattr(self, '_storage_lock', None)
        storage_context = storage_lock if storage_lock is not None else _NULL_CONTEXT
        with storage_context:
            with self._lock:
                self._flush_pending_memtable_rows_for_read()
                self.flush_buffered_writes()
                try:
                    self._storage.create_table(table_name, schema)
                except OSError as e:
                    raise ValueError(str(e)) from e
                self._invalidate_replace_cache()
        self._current_table = table_name
        if schema is None:
            self._schemaless_tables.add(table_name)
        else:
            self._schemaless_tables.discard(table_name)

    def drop_table(self, table_name: str):
        """
        Drop a table and best-effort remove its FTS index files.

        Args:
            table_name: Name of the table to drop.

        Returns:
            None
        """
        self._check_connection()

        # Detach the engine while the table can still be selected.
        if table_name in self._fts_tables:
            try:
                with self._fts_table_context(table_name):
                    self._storage._fts_remove_engine(True)
            except Exception:
                pass

        storage_lock = getattr(self, '_storage_lock', None)
        storage_context = storage_lock if storage_lock is not None else _NULL_CONTEXT
        with storage_context:
            with self._lock:
                self.flush_buffered_writes()
                try:
                    self._storage.drop_table(table_name)
                except (ValueError, RuntimeError):
                    pass
                self._invalidate_replace_cache()
        
        if table_name in self._fts_tables:
            self._fts_tables.pop(table_name, None)
            self._fts_initialized_tables.discard(table_name)
            self._save_fts_config()

        # Best-effort Python-side cleanup in case the engine keeps files open
        try:
            fts_dir = self._dirpath / "fts_indexes"
            for suffix in (".afts", ".afts.wal", ".afts.tmp", ".nfts", ".nfts.wal"):
                path = fts_dir / f"{table_name}{suffix}"
                if path.exists():
                    try:
                        path.unlink()
                    except Exception:
                        pass
        except Exception:
            pass
        
        if self._current_table == table_name:
            self._current_table = None
        self._schemaless_tables.discard(table_name)

    def list_tables(self) -> List[str]:
        """
        List table names in the currently active database.

        Returns:
            List of table name strings.
        """
        self._check_connection()
        with self._lock:
            return self._storage.list_tables()

    def register_temp_table(
        self,
        name: str,
        file_path: str,
        on_bad_lines: str = "error",
        encoding: str = "utf-8",
    ):
        """
        Register a data file (CSV, JSON, Parquet) as a temporary table.

        The file is parsed once and materialized into a native ``.apex`` table
        under a temp directory. Subsequent queries use mmap-backed storage.
        The temp table is cleaned up when the client is closed.

        Args:
            name: Name for the temporary table.
            file_path: Path to the data file (``.csv``/``.tsv``,
                ``.json``/``.ndjson``/``.jsonl``, or ``.parquet``).
            on_bad_lines: CSV malformed-row policy: ``"error"`` (default),
                ``"skip"``, or ``"warn"``. Ignored for non-CSV files.
            encoding: Source text encoding for CSV/TSV files. Non-UTF-8
                input is transcoded once into the private temp area.

        Returns:
            None
        """
        self._check_connection()
        with self._lock:
            self._flush_pending_memtable_rows_for_read()
            self.flush_buffered_writes()
            source_path = file_path
            transcoded_path = None
            lower_path = str(file_path).lower()
            if encoding.lower().replace("_", "-") not in {"utf-8", "utf8"} and lower_path.endswith(
                (".csv", ".tsv")
            ):
                import tempfile

                suffix = ".tsv" if lower_path.endswith(".tsv") else ".csv"
                with open(file_path, "r", encoding=encoding, errors="strict", newline="") as src:
                    text = src.read()
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    newline="",
                    suffix=suffix,
                    delete=False,
                ) as dst:
                    dst.write(text)
                    transcoded_path = dst.name
                source_path = transcoded_path
            try:
                self._storage.register_temp_table(name, source_path, on_bad_lines)
            finally:
                if transcoded_path is not None:
                    try:
                        os.unlink(transcoded_path)
                    except OSError:
                        pass

    def drop_temp_table(self, name: str):
        """
        Drop a previously registered temporary table.

        Args:
            name: Temporary table name.

        Returns:
            None
        """
        self._check_connection()
        with self._lock:
            self._storage.drop_temp_table(name)

    # ============ Compression ============

    def set_compression(self, compression: str) -> bool:
        """Set compression type for the current table.

        Only effective on empty tables (row_count == 0). Ignored if table
        already contains data. The setting persists across restarts.

        Args:
            compression: "none", "lz4", or "zstd".

        Returns:
            True if applied, False if the table is non-empty (no-op).

        Raises:
            ValueError: If *compression* is not a recognised algorithm name.
            RuntimeError: If no table is selected.
        """
        self._check_connection()
        self._ensure_table_selected()
        with self._lock:
            return self._storage.set_compression(compression)

    def get_compression(self) -> str:
        """
        Get the current compression type for the current table.

        Returns:
            ``"none"``, ``"lz4"``, or ``"zstd"``.

        Raises:
            RuntimeError: If no table is selected or the client is closed.
        """
        self._check_connection()
        self._ensure_table_selected()
        with self._lock:
            return self._storage.get_compression()

    # ============ FTS ============

    def init_fts(
        self,
        table_name: str = None,
        index_fields: Optional[List[str]] = None,
        lazy_load: bool = False,
        cache_size: int = 10000
    ) -> 'ApexClient':
        """
        Enable and initialize full-text search for a table.

        Persists configuration to ``fts_config.json`` so FTS auto-enables on reopen.

        Args:
            table_name: Target table. Defaults to the current table.
            index_fields: Optional list of text fields to index. ``None`` indexes
                string fields automatically.
            lazy_load: If ``True``, defer loading index pages until search time.
            cache_size: FTS engine cache size hint (default ``10000``).

        Returns:
            ``self`` for method chaining.
        """
        self._check_connection()
        
        table = table_name or self._current_table
        
        need_switch = table != self._current_table
        original_table = self._current_table if need_switch else None
        
        try:
            if need_switch:
                self.use_table(table)

            available_fields = set(self.list_fields())
            string_fields = {
                field
                for field in available_fields
                if str(self._storage.get_column_dtype(field)).lower() == "string"
            }
            if index_fields is not None:
                if not index_fields:
                    raise ValueError("index_fields must contain at least one text column")
                normalized_fields = list(dict.fromkeys(str(field) for field in index_fields))
                # Schemaless tables may predeclare fields for later dynamic rows.
                # Explicit schemas fail loudly for unknown or non-text fields.
                if available_fields and table not in self._schemaless_tables:
                    unknown = [
                        field for field in normalized_fields
                        if field not in available_fields
                    ]
                    if unknown:
                        raise ValueError(
                            f"Unknown FTS index field(s): {', '.join(unknown)}"
                        )
                    non_text = [
                        field for field in normalized_fields
                        if field not in string_fields
                    ]
                    if non_text:
                        raise ValueError(
                            f"FTS index fields must be text columns: {', '.join(non_text)}"
                        )
                resolved_fields = normalized_fields
            else:
                if available_fields and not string_fields:
                    raise ValueError(
                        f"Table '{table}' has no text columns available for FTS"
                    )
                resolved_fields = None

            previous = self._fts_tables.get(table, {})
            previous_fields = previous.get('index_fields') if isinstance(previous, dict) else None
            fields_changed = (
                isinstance(previous, dict)
                and previous.get('enabled', False)
                and previous_fields != resolved_fields
            )
            if fields_changed:
                # Native engines retain their original field layout. Remove the
                # old index before re-initialising so metadata and postings
                # cannot diverge.
                self._storage._fts_remove_engine(True)
                self._fts_initialized_tables.discard(table)

            new_config = {
                'enabled': True,
                'index_fields': resolved_fields,
                'config': {
                    'lazy_load': lazy_load,
                    'cache_size': cache_size,
                }
            }

            self._storage._init_fts(
                index_fields=resolved_fields,
                lazy_load=lazy_load,
                cache_size=cache_size
            )
            self._fts_initialized_tables.add(table)

            # Publish configuration only after the native index is ready.
            self._fts_tables[table] = new_config
            self._save_fts_config()
            
        finally:
            if need_switch and original_table is not None:
                self.use_table(original_table)
        
        return self

    def _fts_index_from_arrow(self, table: pa.Table, id_column: str = 'id', text_columns: List[str] = None) -> int:
        """Index FTS from an Arrow table using the native Rust ApexFTS engine.
        
        Args:
            table: PyArrow Table with data
            id_column: Column to use as document ID (default 'id')
            text_columns: List of text columns to index (None = all string columns)
            
        Returns:
            Number of documents indexed
        """
        self._check_connection()
        table_name = self._current_table
        
        if not self._is_fts_enabled(table_name):
            raise ValueError(f"FTS not enabled for table '{table_name}'. Call init_fts() first.")
        
        fts_config = self._fts_tables.get(table_name, {})
        if text_columns is None:
            text_columns = fts_config.get('index_fields')
        
        if id_column not in table.column_names:
            id_column = table.column_names[0]
        
        # Determine text columns to index
        if text_columns:
            cols = [c for c in text_columns if c in table.column_names]
        else:
            pa_mod = _ensure_pyarrow()
            cols = [c for c in table.column_names if c != id_column
                    and pa_mod.types.is_string(table.schema.field(c).type)]
        
        if not cols:
            return 0
        
        ids = table.column(id_column).to_pylist()
        columns = {c: [str(v) if v is not None else '' for v in table.column(c).to_pylist()] for c in cols}
        count = self._storage._fts_index_columns(ids, columns)
        self._storage._fts_flush()
        return count

    def _fts_index_from_pandas(self, df: pd.DataFrame, id_column: str = 'id', text_columns: List[str] = None) -> int:
        """Index FTS from a Pandas DataFrame using the native Rust ApexFTS engine.
        
        Args:
            df: Pandas DataFrame with data
            id_column: Column to use as document ID (default 'id')
            text_columns: List of text columns to index (None = all string columns)
            
        Returns:
            Number of documents indexed
        """
        self._check_connection()
        table_name = self._current_table
        
        if not self._is_fts_enabled(table_name):
            raise ValueError(f"FTS not enabled for table '{table_name}'. Call init_fts() first.")
        
        fts_config = self._fts_tables.get(table_name, {})
        if text_columns is None:
            text_columns = fts_config.get('index_fields')
        
        if id_column not in df.columns:
            id_column = df.columns[0]
        
        # Determine text columns to index
        if text_columns:
            cols = [c for c in text_columns if c in df.columns]
        else:
            cols = [c for c in df.columns if c != id_column
                    and df[c].dtype == object]
        
        if not cols:
            return 0
        
        ids = df[id_column].tolist()
        columns = {c: df[c].fillna('').astype(str).tolist() for c in cols}
        count = self._storage._fts_index_columns(ids, columns)
        self._storage._fts_flush()
        return count

    def disable_fts(self, table_name: str = None) -> 'ApexClient':
        """
        Disable FTS for a table while keeping index files on disk.

        Args:
            table_name: Target table. Defaults to the current table.

        Returns:
            ``self`` for method chaining.
        """
        self._check_connection()
        table = table_name or self._current_table

        cfg = self._fts_tables.get(table, {})
        if not isinstance(cfg, dict):
            cfg = {}

        cfg['enabled'] = False
        if 'config' not in cfg or not isinstance(cfg.get('config'), dict):
            cfg['config'] = {}
        self._fts_tables[table] = cfg
        self._save_fts_config()
        return self

    def drop_fts(self, table_name: str = None) -> 'ApexClient':
        """
        Disable FTS for a table and delete its index files.

        Args:
            table_name: Target table. Defaults to the current table.

        Returns:
            ``self`` for method chaining.
        """
        self._check_connection()
        table = table_name or self._current_table

        # Keep config for initialization before deleting it
        prev_cfg = self._fts_tables.get(table)

        # Remove persisted config
        self._fts_tables.pop(table, None)
        self._fts_initialized_tables.discard(table)
        self._save_fts_config()

        # Remove engine and index files in Rust layer while the target table is selected.
        try:
            with self._fts_table_context(table):
                if not self._storage._is_fts_enabled():
                    cfg = prev_cfg.get('config', {}) if isinstance(prev_cfg, dict) else {}
                    index_fields = prev_cfg.get('index_fields') if isinstance(prev_cfg, dict) else None
                    self._storage._init_fts(
                        index_fields=index_fields,
                        lazy_load=bool(cfg.get('lazy_load', False)),
                        cache_size=int(cfg.get('cache_size', 10000)),
                    )
                self._storage._fts_remove_engine(True)
        except Exception:
            pass

        # Best-effort Python-side cleanup in case the engine keeps files open
        try:
            fts_dir = self._dirpath / "fts_indexes"
            for suffix in (".afts", ".afts.wal", ".afts.tmp", ".nfts", ".nfts.wal"):
                path = fts_dir / f"{table}{suffix}"
                if path.exists():
                    try:
                        path.unlink()
                    except Exception:
                        pass
        except Exception:
            pass

        return self

    def _should_index_field(self, field_name: str, field_value, table_name: str = None) -> bool:
        """
        Decide whether a field value should be included in FTS indexing.

        Args:
            field_name: Column/field name.
            field_value: Field value (used when no explicit index field list).
            table_name: Table context. Defaults to the current table.

        Returns:
            ``True`` if the field should be indexed for FTS.
        """
        table = table_name or self._current_table
        
        if not self._is_fts_enabled(table):
            return False
        
        if field_name == '_id':
            return False
        
        fts_config = self._fts_tables.get(table, {})
        index_fields = fts_config.get('index_fields')
        
        if index_fields:
            return field_name in index_fields
        
        return isinstance(field_value, str)

    def _extract_indexable_content(self, data: dict, table_name: str = None) -> dict:
        """
        Extract FTS-indexable string content from a row dict.

        Args:
            data: Row mapping of field name to value.
            table_name: Table context. Defaults to the current table.

        Returns:
            Dict of field name to string content. Empty when FTS is disabled.
        """
        table = table_name or self._current_table
        
        if not self._is_fts_enabled(table):
            return {}
        
        indexable = {}
        for key, value in data.items():
            if self._should_index_field(key, value, table):
                indexable[key] = str(value)
        return indexable

    # ============ Store Operations ============

    def store(self, data) -> None:
        """
        Store one or more rows into the current table.

        Accepts a single row dict, a list of dicts, a columnar ``Dict[str, list]``,
        or pandas / polars / pyarrow tabular objects. Uses scalar fast paths and
        optional client-side buffering when applicable.

        Args:
            data: Row(s) or tabular data to append.

        Returns:
            None

        Raises:
            RuntimeError: If no table is selected or the client is closed.
            ValueError: If *data* is an unsupported type.
        """
        self._check_connection()
        self._ensure_table_selected()
        # Acquire storage lock for thread-safe concurrent access (shared across all clients)
        storage_lock = getattr(self, '_storage_lock', None)
        if storage_lock is not None:
            with storage_lock:
                if isinstance(data, dict):
                    with self._lock:
                        if self._store_scalar_fast_unlocked(data):
                            return
                self._store_impl(data)
        else:
            if isinstance(data, dict):
                with self._lock:
                    if self._store_scalar_fast_unlocked(data):
                        return
            self._store_impl(data)

    def store_durable_one(self, data: dict) -> None:
        """
        Persist one schema-stable row immediately when the durable fast path applies.

        Falls back to :meth:`store` plus :meth:`flush` for unsupported cases so
        the API remains correct when the optimized delta path is unavailable.

        Args:
            data: Single row dict of scalar (non-nested) values.

        Returns:
            None
        """
        self._check_connection()
        self._ensure_table_selected()

        storage_lock = getattr(self, '_storage_lock', None)
        if storage_lock is not None:
            with storage_lock:
                self._store_durable_one_impl(data)
        else:
            self._store_durable_one_impl(data)

    def _store_durable_one_impl(self, data: dict) -> None:
        """
        Internal implementation of :meth:`store_durable_one` (caller holds storage lock).

        Args:
            data: Single row dict.

        Returns:
            None
        """
        with self._lock:
            durable_one = self._store_one_delta_durable
            if (
                durable_one is not None
                and isinstance(data, dict)
                and data
                and all(not isinstance(v, (list, tuple)) and not hasattr(v, 'dtype') for v in data.values())
                and not self._is_fts_enabled(self._current_table)
            ):
                encoded = self._encode_vectors_in_record(data)
                ids = durable_one(encoded)
                if ids is not None:
                    self._has_writes = True
                    self._invalidate_replace_cache()
                    return

            self._store_impl(data)
            self.flush()

    def _store_scalar_fast_unlocked(self, data: dict) -> bool:
        """
        Attempt a lock-held scalar single-row store via buffering or ``store_one`` paths.

        Args:
            data: Single row dict of scalar values.

        Returns:
            ``True`` if the row was stored (or buffered) by a fast path;
            ``False`` if *data* is empty or not scalar-shaped.
        """
        if not data:
            return False
        for value in data.values():
            if isinstance(value, (list, tuple)) or hasattr(value, 'dtype'):
                return False

        fts_enabled = self._is_fts_enabled(self._current_table)
        if self._buffered_writes_enabled and not fts_enabled:
            if self._buffered_write_table is None:
                self._buffered_write_table = self._current_table
            if self._buffered_write_table != self._current_table:
                self._flush_buffered_writes_unlocked()
                self._buffered_write_table = self._current_table
            self._buffered_write_rows.append(data)
            self._has_writes = True
            self._invalidate_replace_cache()
            if (self._buffered_write_flush_rows
                    and len(self._buffered_write_rows) >= self._buffered_write_flush_rows):
                self._flush_buffered_writes_unlocked()
            return True

        store_one = self._store_one
        if store_one is not None and not fts_enabled:
            if self._memtable_single_writes_enabled or self._experimental_memtable_single_writes_enabled:
                store_one_memtable = self._store_one_memtable
                if store_one_memtable is not None:
                    memtable_ids = store_one_memtable(data)
                    if memtable_ids is not None:
                        self._has_writes = True
                        self._invalidate_replace_cache()
                        return True
            if self._experimental_delta_single_writes_enabled:
                store_one_delta = self._store_one_delta
                if store_one_delta is not None:
                    delta_ids = store_one_delta(data)
                    if delta_ids is not None:
                        self._has_writes = True
                        self._invalidate_replace_cache()
                        return True
            store_one(data)
            self._has_writes = True
            self._invalidate_replace_cache()
            return True

        self._storage.store(data)
        self._has_writes = True
        self._invalidate_replace_cache()
        return True
    
    def _store_impl(self, data) -> None:
        """
        Dispatch store logic for the supported tabular and row input shapes.

        Args:
            data: Single row, list of rows, columnar dict, or DataFrame/Table.

        Returns:
            None

        Raises:
            ValueError: If *data* is not a supported type.
        """
        with self._lock:
            # 1. Columnar data Dict[str, list/ndarray]
            if isinstance(data, dict):
                if self._is_columnar_dict(data):
                    self._store_columnar(data)
                    return
        
            data_module = type(data).__module__

            # 2. PyArrow Table - Convert to columnar dict for optimized storage
            if ARROW_AVAILABLE and data_module.startswith("pyarrow"):
                pa_mod = _ensure_pyarrow()
                if not isinstance(data, pa_mod.Table):
                    pa_mod = None
                else:
                    self._store_columnar(self._arrow_table_to_apex_columns(data))
                    return

            # 3. Pandas DataFrame - Convert to columnar dict for optimized storage
            if PANDAS_AVAILABLE and data_module.startswith("pandas"):
                pd_mod = _ensure_pandas()
                if isinstance(data, pd_mod.DataFrame):
                    # Arrow preserves pandas extension nulls, object None values,
                    # and floating NaN-as-null consistently with from_pyarrow.
                    pa_mod = _ensure_pyarrow()
                    table = pa_mod.Table.from_pandas(data, preserve_index=False)
                    self._store_columnar(self._arrow_table_to_apex_columns(table))
                    return

            # 4. Polars DataFrame - Convert to columnar dict for optimized storage
            if POLARS_AVAILABLE and data_module.startswith("polars") and hasattr(data, 'to_dict'):
                _ensure_polars()
                columns = data.to_dict(as_series=False)
                self._store_columnar(columns)
                return

            # 5. Single record dict
            if isinstance(data, dict):
                if self._store_scalar_fast_unlocked(data):
                    return
                self._storage.store(self._encode_vectors_in_record(data))
                self._has_writes = True
                self._invalidate_replace_cache()
                return

            # 6. List[dict] - OPTIMIZED: Convert to columnar for better performance
            elif isinstance(data, list):
                if not data:
                    return
                # Auto-convert to columnar for batch processing (3x faster!)
                if len(data) > 1 and isinstance(data[0], dict):
                    self._store_batch_optimized(data)
                elif isinstance(data[0], dict):
                    # Single-record list: use store() path to handle partial columns correctly
                    self._storage.store(self._encode_vectors_in_record(data[0]))
                    self._has_writes = True
                    self._invalidate_replace_cache()
                else:
                    self._store_batch(data)
                return
            else:
                raise ValueError("Data must be dict, list of dicts, Dict[str, list], pandas.DataFrame, polars.DataFrame, or pyarrow.Table")

    @staticmethod
    def _is_columnar_dict(data: dict) -> bool:
        """
        Return whether *data* is a columnar dict (each value is a sequence/array).

        Args:
            data: Candidate mapping.

        Returns:
            ``True`` if every non-empty value looks like a column sequence.
        """
        if not data:
            return False

        for value in data.values():
            if not (
                isinstance(value, (list, tuple)) or
                (hasattr(value, '__len__') and hasattr(value, 'dtype'))
            ):
                return False

        return True

    def _encode_vectors_in_record(self, record: dict) -> dict:
        """
        Prepare a single record for storage (vector values left intact for Rust schema).

        Args:
            record: Single row dict.

        Returns:
            The same *record* (identity transform for declared vector schemas).
        """
        return record

    def _store_batch(self, records: List[dict]) -> None:
        """
        Store a list of row dicts via the storage batch API.

        Args:
            records: List of row dictionaries.

        Returns:
            None
        """
        if not records:
            return
        self._storage.store_batch(records)
        self._has_writes = True
        self._invalidate_replace_cache()

    def _store_batch_optimized(self, records: List[dict]) -> None:
        """
        Store a batch with automatic columnar conversion for higher throughput.

        Converts a list of dicts into a columnar ``Dict[str, list]`` (missing
        keys become ``None``) and delegates to :meth:`_store_columnar`.

        Args:
            records: List of dict records to store.

        Returns:
            None
        """
        if not records:
            return
        
        # Convert to columnar format for optimal performance.
        # Collect ALL keys across ALL records so missing fields become None (NULL).
        if records and isinstance(records[0], dict):
            all_keys: list = []
            seen_keys: set = set()
            for record in records:
                for k in record:
                    if k not in seen_keys:
                        all_keys.append(k)
                        seen_keys.add(k)
            columns = {key: [record.get(key) for record in records] for key in all_keys}
            self._store_columnar(columns)
        else:
            # Fallback to standard batch store
            self._storage.store_batch(records)

    def _store_columnar(self, columns: Dict[str, list]) -> None:
        """
        Store columnar data via the native ``store_columnar`` path.

        Converts numpy/polars columns to Python lists and encodes mixed-dimension
        vector columns to bytes when needed.

        Args:
            columns: Mapping of column name to value sequences.

        Returns:
            None
        """
        if not columns:
            return
        
        # Convert numpy arrays to Python lists for Rust binding
        converted = {}
        for name, values in columns.items():
            if hasattr(values, 'tolist'):  # numpy array column
                converted[name] = values.tolist()
            elif hasattr(values, 'to_list'):  # polars series
                converted[name] = values.to_list()
            else:
                converted[name] = list(values) if not isinstance(values, list) else values
            if _is_vector_column(converted[name]):
                dims = {
                    len(v)
                    for v in converted[name]
                    if v is not None and isinstance(v, (list, tuple))
                }
                if len(dims) > 1:
                    converted[name] = _encode_vector_col(converted[name])
        
        # Call native columnar storage - much faster than row-by-row
        self._storage.store_columnar(converted)
        self._has_writes = True
        self._invalidate_replace_cache()

    # ============ Query Operations ============

    def _empty_sql_result(self, show_internal_id: bool = None) -> 'ResultView':
        """
        Create an empty :class:`ResultView` for DDL/transaction no-result statements.

        Args:
            show_internal_id: Visibility flag forwarded onto the view.

        Returns:
            Empty :class:`ResultView`.
        """
        rv = ResultView(data=None)
        rv._show_internal_id = show_internal_id
        return rv

    def _start_fast_txn(self, read_only: bool = False, show_internal_id: bool = None) -> 'ResultView':
        """
        Begin a Python-side fast transaction for simple BEGIN/COMMIT workloads.

        Args:
            read_only: Reserved read-only flag for promotion to Rust transactions.
            show_internal_id: Visibility flag for the returned empty result.

        Returns:
            Empty :class:`ResultView`.
        """
        self._in_txn = True
        self._fast_txn_active = True
        self._fast_txn_read_only = read_only
        self._fast_txn_writes = []
        return self._empty_sql_result(show_internal_id)

    def _reset_fast_txn(self) -> None:
        """
        Clear fast-transaction bookkeeping state without touching storage.

        Returns:
            None
        """
        self._fast_txn_active = False
        self._fast_txn_read_only = False
        self._fast_txn_writes = []

    def _promote_fast_txn_to_rust_unlocked(self, begin_sql: str = "BEGIN") -> None:
        """
        Promote a Python fast transaction into a real Rust transaction.

        Replays buffered fast-txn writes under a Rust ``BEGIN``.

        Args:
            begin_sql: BEGIN statement to execute (default ``"BEGIN"``).

        Returns:
            None
        """
        if not self._fast_txn_active:
            return
        writes = self._fast_txn_writes
        read_only = self._fast_txn_read_only
        self._reset_fast_txn()
        self._storage.execute("BEGIN TRANSACTION READ ONLY" if read_only else begin_sql)
        self._in_txn = True
        for write in writes:
            if write[0] == "sql":
                self._storage.execute(write[1])
            elif write[0] == "insert":
                for insert_sql in write[3]:
                    self._storage.execute(insert_sql)

    def _append_fast_txn_insert(self, sql: str) -> bool:
        """
        Buffer a simple INSERT into the active fast transaction.

        Args:
            sql: INSERT statement text.

        Returns:
            ``True`` if the statement was parsed and buffered; ``False`` otherwise.
        """
        parsed = _parse_simple_insert_values(sql)
        if not parsed:
            return False
        table, rows = parsed
        try:
            base_count = int(self._storage.fast_row_count())
        except Exception:
            base_count = 0
        pending_count = len(self._fast_txn_pending_rows(table))
        rows = [dict(row, _id=base_count + pending_count + idx + 1) for idx, row in enumerate(rows)]
        self._fast_txn_writes.append(("insert", table, rows, [sql]))
        return True

    def _store_fast_txn_rows(self, rows: List[dict]) -> None:
        """
        Persist rows collected during a fast transaction commit.

        Prefers delta/batch storage paths when available and FTS is disabled.

        Args:
            rows: Row dicts (``_id`` keys are stripped before write).

        Returns:
            None
        """
        rows = [{k: v for k, v in row.items() if k != "_id"} for row in rows]
        store_rows_delta = getattr(self._storage, "store_rows_delta", None)
        if store_rows_delta is not None and not self._is_fts_enabled(self._current_table):
            ids = store_rows_delta([self._encode_vectors_in_record(row) for row in rows])
            if ids is not None:
                self._has_writes = True
                self._invalidate_replace_cache()
                return
        store_one_delta = getattr(self._storage, "store_one_delta", None)
        if store_one_delta is not None and not self._is_fts_enabled(self._current_table):
            for row in rows:
                ids = store_one_delta(self._encode_vectors_in_record(row))
                if ids is None:
                    self._store_impl(row)
            self._has_writes = True
            self._invalidate_replace_cache()
            return
        if len(rows) == 1:
            self._store_impl(rows[0])
        else:
            self._store_batch_optimized(rows)

    def _commit_fast_txn(self, show_internal_id: bool = None) -> 'ResultView':
        """
        Commit the active Python fast transaction by flushing buffered writes.

        Args:
            show_internal_id: Visibility flag for the returned empty result.

        Returns:
            Empty :class:`ResultView`.
        """
        writes = self._fast_txn_writes
        self._in_txn = False
        self._reset_fast_txn()
        if not writes:
            return self._empty_sql_result(show_internal_id)

        original_table = self._current_table
        pending_by_table = {}
        try:
            for write in writes:
                if write[0] == "insert":
                    _, table, rows, insert_sqls = write
                    if table != self._current_table:
                        self._storage.use_table(table)
                        self._current_table = table
                    if self._storage.has_secondary_indexes():
                        for insert_sql in insert_sqls:
                            self._storage.execute(insert_sql)
                        continue
                    pending_by_table.setdefault(table, []).extend(rows)
                    continue

                for table, rows in pending_by_table.items():
                    if table != self._current_table:
                        self._storage.use_table(table)
                        self._current_table = table
                    self._store_fast_txn_rows(rows)
                pending_by_table.clear()
                self._execute_impl(write[1], show_internal_id=False)

            for table, rows in pending_by_table.items():
                if table != self._current_table:
                    self._storage.use_table(table)
                    self._current_table = table
                self._store_fast_txn_rows(rows)
        finally:
            if original_table and original_table != self._current_table:
                self._storage.use_table(original_table)
                self._current_table = original_table
        return self._empty_sql_result(show_internal_id)

    def _rollback_fast_txn(self, show_internal_id: bool = None) -> 'ResultView':
        """
        Roll back the active Python fast transaction and discard buffered writes.

        Args:
            show_internal_id: Visibility flag for the returned empty result.

        Returns:
            Empty :class:`ResultView`.
        """
        self._in_txn = False
        self._reset_fast_txn()
        return self._empty_sql_result(show_internal_id)

    def _fast_txn_pending_rows(self, table_name: str = None) -> List[dict]:
        """
        Return pending insert rows for a table inside the fast transaction.

        Args:
            table_name: Table to filter. Defaults to the current table.

        Returns:
            List of pending row dicts (may include provisional ``_id`` values).
        """
        table_name = table_name or self._current_table
        rows = []
        for write in self._fast_txn_writes:
            if write[0] == "insert" and table_name and write[1].lower() == table_name.lower():
                rows.extend(write[2])
        return rows

    def _project_rows(self, rows: List[dict], columns: Optional[List[str]]) -> List[dict]:
        """
        Project row dicts onto a subset of columns.

        Args:
            rows: Input row dictionaries.
            columns: Column names to keep, or ``None`` to keep all columns.

        Returns:
            List of projected row dictionaries.
        """
        if not columns:
            return [dict(row) for row in rows]
        return [{col: row.get(col) for col in columns} for row in rows]

    def _fast_txn_select(self, sql: str, show_internal_id: bool = None):
        """
        Answer a simple SELECT against committed data plus fast-txn pending inserts.

        Args:
            sql: SELECT statement text.
            show_internal_id: Whether to expose ``_id`` in the result.

        Returns:
            :class:`ResultView` when handled; ``None`` when the statement is not
            eligible for the fast-txn select path.
        """
        table = _simple_from_table(sql) or self._current_table
        pending_rows = self._fast_txn_pending_rows(table)
        old_in_txn = self._in_txn
        old_fast_txn_active = self._fast_txn_active
        self._in_txn = False
        self._fast_txn_active = False
        try:
            if not pending_rows:
                return self._execute_impl(sql, show_internal_id)

            count_match = _RE_SIMPLE_COUNT_STAR.match(sql)
            if count_match:
                base = self._execute_impl(sql, show_internal_id=False).scalar()
                rv = ResultView(lazy_pydict={count_match.group(1) or "COUNT(*)": [base + len(pending_rows)]})
                rv._show_internal_id = False
                return rv

            columns = _simple_projection_columns(sql)
            string_eq = _RE_SIMPLE_STRING_EQ.search(sql) or _RE_SIMPLE_STRING_EQ_LIMIT.search(sql)
            if string_eq:
                filter_col, filter_val = string_eq.group(1), string_eq.group(2)
                pending = [row for row in pending_rows if str(row.get(filter_col)) == filter_val]
                base_rows = self._execute_impl(sql, show_internal_id).to_dict() or []
                rows = base_rows + self._project_rows(pending, columns)
                if string_eq.re is _RE_SIMPLE_STRING_EQ_LIMIT:
                    limit_val = int(string_eq.group(3))
                    offset_val = int(string_eq.group(4) or 0)
                    rows = rows[offset_val:offset_val + limit_val]
                rv = ResultView(data=rows or None)
                rv._show_internal_id = show_internal_id
                return rv

            if "WHERE" not in sql.upper():
                base_rows = self._execute_impl(sql, show_internal_id).to_dict() or []
                rows = base_rows + self._project_rows(pending_rows, columns)
                rv = ResultView(data=rows or None)
                rv._show_internal_id = show_internal_id
                return rv
        finally:
            self._in_txn = old_in_txn
            self._fast_txn_active = old_fast_txn_active
        return None

    def execute(
        self,
        sql: str,
        show_internal_id: bool = None,
        params=None,
    ) -> 'ResultView':
        """
        Execute a SQL statement and return a :class:`ResultView`.

        Supports SELECT/DML/DDL/transaction statements. Uses Python-side caches
        and fast paths for common point lookups, scans, and simple filters before
        falling back to the Rust executor.

        Args:
            sql: SQL statement to execute.
            show_internal_id: If ``True``, include the internal ``_id`` column
                in SELECT results when applicable. ``None`` uses default policy.
            params: Optional positional parameters. Parameter binding currently
                supports the TopK vector query shape with one ``?`` placeholder;
                the query vector is passed directly to the native FFI path
                without formatting or reparsing a large SQL array literal.

        Returns:
            :class:`ResultView` containing query rows or an empty view for
            statements without a result set.
        """
        self._check_connection()

        if params is not None:
            match = _RE_BOUND_TOPK.match(sql)
            if match is None:
                raise ValueError(
                    "parameter binding currently supports only "
                    "explode_rename(topk_distance(column, ?, k, metric), ...)"
                )
            try:
                bound = list(params)
            except TypeError as exc:
                raise ValueError("params must be an iterable with one query vector") from exc
            if len(bound) != 1:
                raise ValueError("the bound TopK query requires exactly one parameter")
            col, k_text, metric, id_col, dist_col, table = match.groups()
            self._ensure_table_selected()
            if self._current_table is None or table.lower() != self._current_table.lower():
                raise ValueError(
                    f"bound TopK query targets table {table!r}, "
                    f"but the selected table is {self._current_table!r}"
                )
            return self.topk_distance(
                col,
                bound[0],
                k=int(k_text),
                metric=metric,
                id_col=id_col,
                dist_col=dist_col,
            )

        if sql == "BEGIN" and not getattr(self, '_in_txn', False):
            return self._start_fast_txn(show_internal_id=show_internal_id)
        if getattr(self, '_fast_txn_active', False):
            if sql == "COMMIT":
                return self._commit_fast_txn(show_internal_id)
            if sql == "ROLLBACK":
                return self._rollback_fast_txn(show_internal_id)

        cached_simple = self._simple_sql_cache.get(sql)
        if cached_simple:
            if (not getattr(self, '_in_txn', False)
                    and not getattr(self, '_fast_txn_active', False)
                    and cached_simple[0] in ('point', 'projected_point', 'numeric_range_limit')):
                try:
                    kind = cached_simple[0]
                    table_name = cached_simple[1]
                    current_table = self._current_table
                    if current_table and (
                            table_name == current_table
                            or table_name.lower() == current_table.lower()):
                        show_flag = bool(show_internal_id) if show_internal_id is not None else False
                        if kind == 'point':
                            row = self._storage.retrieve(cached_simple[2])
                            if row is not None:
                                if not show_flag and '_id' in row:
                                    row = {k: v for k, v in row.items() if k != '_id'}
                                rv = ResultView(data=[row])
                                rv._show_internal_id = show_flag
                                return rv
                        if kind == 'projected_point':
                            row = self._storage.retrieve_projected_row(
                                cached_simple[2], list(cached_simple[3])
                            )
                            if row is None:
                                row = self._recover_projected_point_row(
                                    cached_simple[2], cached_simple[3]
                                )
                            if row is not None:
                                rv = ResultView(data=[row])
                                rv._show_internal_id = show_flag
                                return rv
                        if kind == 'numeric_range_limit':
                            _, _, filter_col, op, value, limit_val, offset_val = cached_simple
                            cache_key = (
                                self._current_database, current_table, sql, show_flag
                            )
                            token = (self._numeric_range_cache_token()
                                     if limit_val <= 256 else None)
                            cached_rows = self._numeric_range_rows_cache.get(cache_key)
                            if (token is not None and cached_rows is not None
                                    and cached_rows[0] == token):
                                rv = ResultView(data=[row.copy() for row in cached_rows[1]])
                                rv._show_internal_id = show_flag
                                return rv
                            result = self._storage.retrieve_by_numeric_range_limit(
                                filter_col, op, value, limit_val, offset_val
                            )
                            if isinstance(result, dict):
                                columns_dict = result.get('columns_dict')
                                if columns_dict is not None:
                                    rv = self._result_view_from_columns_dict(
                                        sql, columns_dict, show_flag
                                    )
                                    if token is not None:
                                        rows = rv.to_dict()
                                        if len(self._numeric_range_rows_cache) >= 16:
                                            self._numeric_range_rows_cache.pop(
                                                next(iter(self._numeric_range_rows_cache))
                                            )
                                        self._numeric_range_rows_cache[cache_key] = (
                                            token,
                                            tuple(row.copy() for row in rows),
                                        )
                                    return rv
                except Exception:
                    pass
            hot_result = self._execute_cached_simple_select(sql, cached_simple, show_internal_id)
            if hot_result is not _HOT_CACHE_MISS:
                return hot_result

        if (not getattr(self, '_in_txn', False)
                and not getattr(self, '_fast_txn_active', False)):
            view_result = self._execute_simple_group_view_select(
                sql,
                bool(show_internal_id) if show_internal_id is not None else False,
            )
            if view_result is not _HOT_CACHE_MISS:
                return view_result
        
        # Lock-free execution: Rust layer handles concurrent reads via RwLock.
        # Python-level _storage_lock was causing serialization of all queries.
        return self._execute_impl(sql, show_internal_id)

    def _flush_pending_memtable_rows_for_read(self) -> None:
        """
        Persist storage-level single-row write buffers before broad reads.

        Returns:
            None
        """
        has_pending = getattr(self._storage, "has_pending_memtable_rows", None)
        if has_pending is None:
            return
        try:
            if has_pending():
                self._storage.flush()
        except Exception:
            # Reads should still fall through to their normal error handling.
            pass

    def _flush_pending_overlay_writes_unlocked(self) -> None:
        """
        Persist same-client buffered/overlay writes before SQL write execution.

        Returns:
            None
        """
        self._flush_buffered_writes_unlocked()
        has_pending = getattr(self._storage, "has_pending_overlay_writes", None)
        if has_pending is None:
            return
        if has_pending():
            self._storage.flush()

    @staticmethod
    def _should_use_columnar_materialization(sql_upper: str, sig: str) -> bool:
        """
        Decide whether Rust should return columnar Python lists for a result set.

        Prefers columnar conversion for ``to_dict``-friendly analytic / filtered
        SELECT shapes to avoid Arrow import and ``Table.to_pylist()`` overhead.

        Args:
            sql_upper: Upper-cased SQL text.
            sig: Route signature from :func:`_classify_sql_route`.

        Returns:
            ``True`` when columnar materialization is preferred.
        """
        if sig not in ('like', 'complex', 'projected_full_scan', 'table_func'):
            return False
        is_cte_query = sql_upper.startswith('WITH ')
        if not (sql_upper.startswith('SELECT') or is_cte_query):
            return False
        if any(token in sql_upper for token in ('UNION', 'INTERSECT', 'EXCEPT')):
            return False
        if not is_cte_query and 'JOIN' in sql_upper:
            return False

        # Analytic CTEs usually return compact derived result sets that execute()
        # callers immediately consume as Python rows. Building column lists in
        # Rust avoids Arrow import + Table.to_pylist() overhead on repeated runs.
        if is_cte_query:
            return any(token in sql_upper for token in (
                'GROUP', 'HAVING', 'DISTINCT', 'ORDER', ' OVER ', 'JOIN',
                'COUNT(', 'SUM(', 'AVG(', 'MIN(', 'MAX(',
            ))

        # Projected full scan: SELECT col1, col2 FROM table (no WHERE/LIMIT/etc.)
        if (sig == 'projected_full_scan'):
            return True

        has_agg = ('COUNT(' in sql_upper or 'SUM(' in sql_upper or 'AVG(' in sql_upper
                   or 'MIN(' in sql_upper or 'MAX(' in sql_upper)

        if (sig == 'complex' and has_agg
                and 'WHERE' in sql_upper and "'" in sql_upper
                and not any(token in sql_upper for token in (
                    'GROUP', 'HAVING', 'ORDER', 'DISTINCT', 'LIMIT',
                ))):
            return False

        # Large filtered row sets avoid PyArrow Table.to_pylist() overhead.
        if (sql_upper.startswith('SELECT *')
                and 'WHERE' in sql_upper
                and not any(token in sql_upper for token in ('GROUP', 'HAVING', 'ORDER', 'DISTINCT'))):
            return True

        # Small/medium OLAP outputs are usually consumed as Python rows in execute().to_dict().
        # Let Rust's executor fast paths return columnar Python lists directly instead of
        # importing Arrow then converting rows.
        if ('GROUP' in sql_upper or 'HAVING' in sql_upper or 'DISTINCT' in sql_upper
                or has_agg):
            return True
        if 'ORDER' in sql_upper and 'LIMIT' in sql_upper:
            return True
        if ' OVER ' in sql_upper and 'LIMIT' in sql_upper:
            return True
        return False

    @staticmethod
    def _extract_point_lookup_id(sql: str) -> Optional[int]:
        """
        Extract the integer ``_id`` from a simple ``WHERE _id = N`` clause.

        Args:
            sql: SQL statement text.

        Returns:
            Integer ID, or ``None`` if not found / invalid.
        """
        match = _RE_POINT_LOOKUP_ID.search(sql)
        if not match:
            return None
        try:
            return int(match.group(1))
        except (TypeError, ValueError):
            return None

    def _execute_cached_simple_select(self, sql, cached_simple, show_internal_id):
        """
        Execute a previously classified simple SELECT via storage hot paths.

        Args:
            sql: Original SQL text.
            cached_simple: Cached classification tuple from ``_simple_sql_cache``.
            show_internal_id: Whether to expose ``_id``.

        Returns:
            :class:`ResultView` on cache hit/success, or the sentinel
            ``_HOT_CACHE_MISS`` when the hot path does not apply.
        """
        if (not cached_simple
                or getattr(self, '_in_txn', False)
                or getattr(self, '_fast_txn_active', False)):
            return _HOT_CACHE_MISS
        kind = cached_simple[0]
        if kind not in (
                'count', 'point', 'projected_point', 'batch', 'projected_batch',
                'scan_limit', 'projected_scan_limit',
                'projected_full_scan', 'string_eq', 'projected_string_eq',
                'columnar_select', 'string_eq_limit1',
                'projected_string_eq_limit1', 'projected_string_eq_limit',
                'numeric_range_limit', 'numeric_filtered_agg',
                'string_filtered_agg',
        ):
            return _HOT_CACHE_MISS
        try:
            if not self._current_table:
                self._ensure_table_selected()
            table_name = cached_simple[1]
            show_flag = bool(show_internal_id) if show_internal_id is not None else False
            if kind == 'string_filtered_agg':
                if not self._current_table or table_name.lower() != self._current_table.lower():
                    return _HOT_CACHE_MISS
                result = self._storage.execute(sql)
                if isinstance(result, dict):
                    columns_dict = result.get('columns_dict')
                    if columns_dict is not None:
                        return self._result_view_from_columns_dict(sql, columns_dict, show_flag)
                return _HOT_CACHE_MISS

            if kind == 'numeric_filtered_agg':
                _, table_name, filter_col, op, value = cached_simple
                result = self._storage.execute_filtered_numeric_agg(
                    sql, table_name, filter_col, op, value
                )
                if isinstance(result, dict):
                    columns_dict = result.get('columns_dict')
                    if columns_dict is not None:
                        return self._result_view_from_columns_dict(sql, columns_dict, show_flag)
                return _HOT_CACHE_MISS

            if not self._current_table or table_name.lower() != self._current_table.lower():
                return _HOT_CACHE_MISS

            if kind == 'count':
                rv = ResultView(lazy_pydict={
                    cached_simple[2] or 'COUNT(*)': [self._storage.fast_row_count()]
                })
                rv._show_internal_id = False
                return rv

            if kind in ('batch', 'projected_batch'):
                ids = list(cached_simple[2])
                result = (self._storage.retrieve_many(ids) if kind == 'batch'
                          else self._storage.retrieve_many_projected(ids, list(cached_simple[3])))
                if isinstance(result, dict):
                    columns_dict = result.get('columns_dict')
                    if columns_dict is not None:
                        return self._result_view_from_columns_dict(sql, columns_dict, show_flag)
                return _HOT_CACHE_MISS

            if kind in (
                    'scan_limit', 'projected_scan_limit',
                    'projected_full_scan', 'string_eq', 'projected_string_eq',
                    'columnar_select',
            ):
                result = self._storage.execute(sql)
                if isinstance(result, dict):
                    columns_dict = result.get('columns_dict')
                    if columns_dict is not None:
                        if not columns_dict:
                            return _HOT_CACHE_MISS
                        if not next(iter(columns_dict.values()), []):
                            requested = _simple_projection_columns(sql)
                            if requested:
                                available = set(self.list_fields())
                                if any(column not in available for column in requested):
                                    return _HOT_CACHE_MISS
                            rv = ResultView(lazy_pydict=columns_dict)
                            rv._show_internal_id = show_flag
                            return rv
                        return self._result_view_from_columns_dict(sql, columns_dict, show_flag)
                return _HOT_CACHE_MISS

            if kind == 'numeric_range_limit':
                _, _, filter_col, op, value, limit_val, offset_val = cached_simple
                result = self._storage.retrieve_by_numeric_range_limit(
                    filter_col, op, value, limit_val, offset_val
                )
                if isinstance(result, dict):
                    columns_dict = result.get('columns_dict')
                    if columns_dict is not None:
                        return self._result_view_from_columns_dict(sql, columns_dict, show_flag)
                return _HOT_CACHE_MISS

            if kind == 'string_eq_limit1':
                _, _, filter_col, filter_val, _ = cached_simple
                result = self._storage.retrieve_first_by_string_eq_limit1(filter_col, filter_val)
                if result is not None:
                    columns_dict = result.get('columns_dict')
                    if columns_dict is not None:
                        return self._result_view_from_columns_dict(sql, columns_dict, show_flag)
                return _HOT_CACHE_MISS

            if kind == 'projected_string_eq_limit1':
                _, _, filter_col, filter_val, columns = cached_simple
                result = self._storage.retrieve_projected_first_by_string_eq_limit1(
                    filter_col, filter_val, list(columns)
                )
                if result is not None:
                    columns_dict = result.get('columns_dict')
                    if columns_dict is not None:
                        return self._result_view_from_columns_dict(sql, columns_dict, show_flag)
                return _HOT_CACHE_MISS

            if kind == 'projected_string_eq_limit':
                _, _, filter_col, filter_val, columns, limit_val, offset_val = cached_simple
                result = self._storage.retrieve_projected_by_string_eq_limit(
                    filter_col, filter_val, list(columns), limit_val, offset_val
                )
                if result is not None:
                    columns_dict = result.get('columns_dict')
                    if columns_dict is not None:
                        return self._result_view_from_columns_dict(sql, columns_dict, show_flag)
                return _HOT_CACHE_MISS

            point_id = cached_simple[2]
            if kind == 'point':
                show_internal_id = show_flag
                row = self._storage.retrieve(point_id)
                if row is None:
                    if self._pending_memtable_point_miss():
                        rv = ResultView(data=None)
                        rv._show_internal_id = show_internal_id
                        return rv
                    return _HOT_CACHE_MISS
                if not show_internal_id and '_id' in row:
                    row = {k: v for k, v in row.items() if k != '_id'}
            else:
                row = self._storage.retrieve_projected_row(point_id, list(cached_simple[3]))
                show_internal_id = show_flag
                if row is None:
                    row = self._recover_projected_point_row(point_id, cached_simple[3])
                if row is None:
                    if self._pending_memtable_point_miss():
                        rv = ResultView(data=None)
                        rv._show_internal_id = show_internal_id
                        return rv
                    return _HOT_CACHE_MISS
            rows = [row]
            rv = ResultView(data=rows)
            rv._show_internal_id = show_internal_id
            return rv
        except Exception:
            return _HOT_CACHE_MISS

    @staticmethod
    def _simple_identifier_list(text: str) -> Optional[List[str]]:
        """
        Parse a comma-separated list of simple SQL identifiers.

        Args:
            text: Identifier list text (e.g. SELECT projection fragment).

        Returns:
            List of identifiers, or ``None`` if any token is not a plain identifier.
        """
        cols = []
        for part in text.split(','):
            col = part.strip().strip('"').strip('`')
            if not col or not re.match(r"^[A-Za-z_]\w*$", col):
                return None
            cols.append(col)
        return cols

    @staticmethod
    def _view_aggregate_sql(agg: dict) -> Optional[str]:
        """
        Build SQL for a recognized view-aggregate rewrite descriptor.

        Args:
            agg: Aggregate descriptor dict produced by the group-view fast path.

        Returns:
            SQL string to execute, or ``None`` if *agg* is unsupported.
        """
        func = str(agg.get("func", "")).upper()
        if func not in {"COUNT", "AVG", "SUM", "MIN", "MAX"}:
            return None
        col = agg.get("column")
        if func == "COUNT" and col is None:
            inner = "*"
        elif isinstance(col, str) and re.match(r"^[A-Za-z_]\w*$", col):
            inner = col
        else:
            return None
        alias = agg.get("alias")
        if not isinstance(alias, str) or not re.match(r"^[A-Za-z_]\w*$", alias):
            return None
        return f"{func}({inner}) AS {alias}"

    def _execute_simple_group_view_select(self, sql: str, show_internal_id: bool):
        """
        Try a specialized fast path for simple grouped-view SELECT patterns.

        Args:
            sql: SQL statement text.
            show_internal_id: Whether to expose ``_id``.

        Returns:
            :class:`ResultView` on success, or ``_HOT_CACHE_MISS`` when inapplicable.
        """
        sql_lower = sql.lower()
        if (" where " not in sql_lower
                or " order by " not in sql_lower
                or " limit " not in sql_lower):
            return _HOT_CACHE_MISS
        match = _RE_SIMPLE_GROUP_VIEW_OUTER.match(sql)
        if not match:
            return _HOT_CACHE_MISS
        selected = self._simple_identifier_list(match.group(1))
        if not selected:
            return _HOT_CACHE_MISS

        view_name = match.group(2).lower()
        where_alias = match.group(3)
        op = match.group(4)
        threshold = float(match.group(5))
        order_alias = match.group(6)
        limit = int(match.group(7))
        if where_alias.lower() != order_alias.lower() or limit <= 0:
            return _HOT_CACHE_MISS

        view_stmt = self._load_view_catalog().get(view_name)
        if not isinstance(view_stmt, dict):
            return _HOT_CACHE_MISS

        from_item = view_stmt.get("from")
        table = None
        if isinstance(from_item, dict):
            table_item = from_item.get("Table")
            if isinstance(table_item, dict):
                table = table_item.get("table")
        if not isinstance(table, str) or not re.match(r"^[A-Za-z_]\w*$", table):
            return _HOT_CACHE_MISS

        group_by = view_stmt.get("group_by")
        if not isinstance(group_by, list) or len(group_by) != 1:
            return _HOT_CACHE_MISS
        group_col = group_by[0]
        if not isinstance(group_col, str) or not re.match(r"^[A-Za-z_]\w*$", group_col):
            return _HOT_CACHE_MISS

        alias_exprs = {group_col.lower(): group_col}
        alias_names = {group_col.lower(): group_col}
        for col in view_stmt.get("columns", []):
            if not isinstance(col, dict):
                return _HOT_CACHE_MISS
            if "Column" in col:
                name = col["Column"]
                if isinstance(name, str) and name == group_col:
                    alias_exprs[name.lower()] = name
                    alias_names[name.lower()] = name
                else:
                    return _HOT_CACHE_MISS
            elif "Aggregate" in col:
                agg = col["Aggregate"]
                if not isinstance(agg, dict):
                    return _HOT_CACHE_MISS
                expr = self._view_aggregate_sql(agg)
                alias = agg.get("alias")
                if expr is None or not isinstance(alias, str):
                    return _HOT_CACHE_MISS
                alias_exprs[alias.lower()] = expr
                alias_names[alias.lower()] = alias
            else:
                return _HOT_CACHE_MISS

        needed_keys = []
        for name in [*selected, where_alias, order_alias]:
            key = name.lower()
            if key not in alias_exprs:
                return _HOT_CACHE_MISS
            if key not in needed_keys:
                needed_keys.append(key)

        select_parts = []
        for key in [group_col.lower(), *needed_keys]:
            if key in alias_exprs and alias_exprs[key] not in select_parts:
                select_parts.append(alias_exprs[key])
        pruned_sql = (
            f"SELECT {', '.join(select_parts)} FROM {table} "
            f"GROUP BY {group_col} ORDER BY {alias_names[order_alias.lower()]} DESC LIMIT {limit}"
        )

        try:
            result = self._storage.execute(pruned_sql)
            if not isinstance(result, dict):
                return _HOT_CACHE_MISS
            columns_dict = result.get('columns_dict')
            if columns_dict is None:
                return _HOT_CACHE_MISS

            order_col = alias_names[order_alias.lower()]
            order_values = list(columns_dict.get(order_col, []))
            if op == ">":
                keep = [i for i, value in enumerate(order_values) if value > threshold]
            else:
                keep = [i for i, value in enumerate(order_values) if value >= threshold]

            projected = {}
            for name in selected:
                out_name = alias_names[name.lower()]
                values = list(columns_dict.get(out_name, []))
                projected[out_name] = [values[i] for i in keep]

            rv = ResultView(lazy_pydict=projected)
            rv._show_internal_id = show_internal_id
            return rv
        except Exception:
            return _HOT_CACHE_MISS
    
    def _execute_impl(self, sql: str, show_internal_id: bool = None) -> 'ResultView':
        """
        Core SQL execution implementation with routing, caching, and storage calls.

        Args:
            sql: SQL statement text.
            show_internal_id: Whether to expose ``_id`` (``None`` = default policy).

        Returns:
            :class:`ResultView` for the statement result.
        """
        sql_upper = sql.strip().upper()

        if (not getattr(self, '_in_txn', False)
                and (sql_upper == 'BEGIN' or sql_upper == 'BEGIN;' or sql_upper.startswith('BEGIN TRANSACTION'))):
            return self._start_fast_txn(read_only='READ ONLY' in sql_upper, show_internal_id=show_internal_id)

        if not getattr(self, '_in_txn', False):
            cached_update = self._simple_sql_cache.get(sql)
            if cached_update is None:
                update_match = _RE_SIMPLE_NUMERIC_UPDATE_BY_ID.match(sql)
                if update_match:
                    try:
                        value_text = update_match.group(3)
                        value = float(value_text) if "." in value_text else int(value_text)
                        cached_update = (
                            'update_numeric_by_id',
                            update_match.group(1),
                            update_match.group(2),
                            value,
                            int(update_match.group(4)),
                        )
                        if len(self._simple_sql_cache) >= 256:
                            self._simple_sql_cache.clear()
                        self._simple_sql_cache[sql] = cached_update
                    except (TypeError, ValueError):
                        cached_update = False

            if cached_update and cached_update[0] == 'update_numeric_by_id':
                try:
                    _, table_name, col_name, value, row_id = cached_update
                    with self._lock:
                        self._ensure_table_selected()
                        if (self._current_table and table_name.lower() == self._current_table.lower()
                                and col_name != "_id"):
                            update_key = (
                                self._current_database,
                                self._current_table,
                                int(row_id),
                                str(col_name),
                                value,
                            )
                            # Repeated idempotent updates are common in the OLTP microbenchmarks.
                            # If we already proved the exact same write is a no-op, skip the
                            # overlay flush check and return immediately.
                            if self._last_exact_numeric_update == update_key:
                                rv = ResultView(lazy_pydict={
                                    "rows_affected": [self._last_exact_numeric_update_result]
                                })
                                rv._show_internal_id = False
                                return rv
                            self._flush_pending_overlay_writes_unlocked()
                            updated = self._storage.update_numeric_by_id_inplace(row_id, col_name, value)
                            if updated is not None:
                                if updated:
                                    self._has_writes = True
                                    self._last_exact_replace_key = None
                                    self._last_exact_replace_data = None
                                self._remember_exact_numeric_update(row_id, col_name, value, updated)
                                rv = ResultView(lazy_pydict={"rows_affected": [updated]})
                                rv._show_internal_id = False
                                return rv
                except Exception:
                    pass  # fall through to the general SQL executor

            cached_simple = self._simple_sql_cache.get(sql)
            if cached_simple is None:
                cached_simple = False
                cacheable_select = (
                    sql_upper.startswith('SELECT')
                    and ';' not in sql.strip().rstrip(';')
                )
                count_match = _RE_SIMPLE_COUNT_STAR.match(sql) if cacheable_select else None
                if count_match:
                    cached_simple = ('count', count_match.group(2), count_match.group(1))

                point_match = (
                    _RE_SIMPLE_POINT_LOOKUP.match(sql)
                    if cacheable_select and not cached_simple else None
                )
                if point_match:
                    cached_simple = ('point', point_match.group(1), int(point_match.group(2)), None)
                else:
                    projected_point = (
                        _RE_SIMPLE_PROJECTED_POINT_LOOKUP.match(sql)
                        if cacheable_select else None
                    )
                    if projected_point:
                        columns = _projection_columns_from_text(projected_point.group(1).strip())
                        if columns:
                            cached_simple = (
                                'projected_point',
                                projected_point.group(2),
                                int(projected_point.group(3)),
                                tuple(columns),
                            )
                    elif cacheable_select:
                        table_name = _simple_from_table(sql)
                        ids = _simple_id_list(sql) if table_name else None
                        columns = _simple_projection_columns(sql)
                        if sql_upper.startswith('SELECT *') and ids and table_name:
                            cached_simple = ('batch', table_name, tuple(sorted(set(ids))), None)
                        elif columns and ids and table_name:
                            cached_simple = ('projected_batch', table_name, tuple(ids), tuple(columns))
                        else:
                            scan_match = (
                                _RE_SIMPLE_SCAN_SHAPE.match(sql)
                                if (table_name and not ids and not cached_simple
                                    and (columns or sql_upper.startswith('SELECT *')))
                                else None
                            )
                            if scan_match:
                                projection = scan_match.group(1).strip()
                                limit_text = scan_match.group(3)
                                offset_val = int(scan_match.group(4) or 0)
                                if projection == '*' and limit_text is not None:
                                    cached_simple = (
                                        'scan_limit', table_name, int(limit_text), offset_val
                                    )
                                elif columns:
                                    cached_simple = (('projected_scan_limit', table_name,
                                                      int(limit_text), offset_val, tuple(columns))
                                                     if limit_text is not None
                                                     else ('projected_full_scan', table_name, tuple(columns)))

                            string_limit = (None if cached_simple
                                            else _RE_SIMPLE_STRING_EQ_LIMIT.search(sql))
                            string_eq = (None if cached_simple or string_limit
                                         else _RE_SIMPLE_STRING_EQ.search(sql))
                            if string_eq and table_name:
                                if columns:
                                    cached_simple = (
                                        'projected_string_eq', table_name, tuple(columns),
                                        string_eq.group(1), string_eq.group(2),
                                    )
                                elif sql_upper.startswith('SELECT *'):
                                    cached_simple = (
                                        'string_eq', table_name,
                                        string_eq.group(1), string_eq.group(2),
                                    )
                            string_limit_table = _simple_from_table(sql) if string_limit else None
                            if string_limit and string_limit_table:
                                try:
                                    limit_val = int(string_limit.group(3))
                                    offset_val = int(string_limit.group(4) or 0)
                                except (TypeError, ValueError):
                                    limit_val = -1
                                    offset_val = -1
                                if (limit_val == 1 and offset_val == 0
                                        and 'ORDER' not in sql_upper
                                        and 'GROUP' not in sql_upper and 'JOIN' not in sql_upper
                                        and 'BETWEEN' not in sql_upper and ' IN ' not in sql_upper
                                        and ' LIKE ' not in sql_upper
                                        and ' AND ' not in sql_upper and ' OR ' not in sql_upper
                                        and '>' not in sql_upper and '<' not in sql_upper):
                                    filter_col = string_limit.group(1)
                                    filter_val = string_limit.group(2)
                                    if columns:
                                        cached_simple = (
                                            'projected_string_eq_limit1',
                                            string_limit_table,
                                            filter_col,
                                            filter_val,
                                            tuple(columns),
                                        )
                                    elif sql_upper.startswith('SELECT *'):
                                        cached_simple = (
                                            'string_eq_limit1',
                                            string_limit_table,
                                            filter_col,
                                            filter_val,
                                            None,
                                        )
                                elif (limit_val >= 0 and offset_val >= 0 and columns
                                      and 'ORDER' not in sql_upper
                                      and 'GROUP' not in sql_upper and 'JOIN' not in sql_upper
                                      and 'BETWEEN' not in sql_upper and ' IN ' not in sql_upper
                                      and ' LIKE ' not in sql_upper
                                      and ' AND ' not in sql_upper and ' OR ' not in sql_upper
                                      and '>' not in sql_upper and '<' not in sql_upper):
                                    cached_simple = (
                                        'projected_string_eq_limit',
                                        string_limit_table,
                                        string_limit.group(1),
                                        string_limit.group(2),
                                        tuple(columns),
                                        limit_val,
                                        offset_val,
                                    )
                            elif not columns:
                                numeric_limit = _RE_SIMPLE_NUMERIC_RANGE_LIMIT.match(sql)
                                if numeric_limit and numeric_limit.group(2).lower() != '_id':
                                    try:
                                        limit_val = int(numeric_limit.group(5))
                                        offset_val = int(numeric_limit.group(6) or 0)
                                        value = float(numeric_limit.group(4))
                                        cached_simple = (
                                            'numeric_range_limit',
                                            numeric_limit.group(1),
                                            numeric_limit.group(2),
                                            numeric_limit.group(3),
                                            value,
                                            limit_val,
                                            offset_val,
                                        )
                                    except (TypeError, ValueError):
                                        pass
                                if not cached_simple:
                                    string_agg = _RE_SIMPLE_STRING_FILTERED_AGG.match(sql)
                                    if (string_agg
                                            and string_agg.group(3).lower() != '_id'
                                            and _RE_AGGREGATE_FUNC.search(string_agg.group(1))
                                            and 'DISTINCT' not in sql_upper):
                                        cached_simple = (
                                            'string_filtered_agg',
                                            string_agg.group(2),
                                            string_agg.group(3),
                                            string_agg.group(4),
                                        )
                                if not cached_simple:
                                    numeric_agg = _RE_SIMPLE_NUMERIC_FILTERED_AGG.match(sql)
                                    if (numeric_agg
                                            and numeric_agg.group(3).lower() != '_id'
                                            and _RE_AGGREGATE_FUNC.search(numeric_agg.group(1))
                                            and 'DISTINCT' not in sql_upper):
                                        try:
                                            cached_simple = (
                                                'numeric_filtered_agg',
                                                numeric_agg.group(2),
                                                numeric_agg.group(3),
                                                numeric_agg.group(4),
                                                float(numeric_agg.group(5)),
                                            )
                                        except (TypeError, ValueError):
                                            pass
                if len(self._simple_sql_cache) >= 256:
                    self._simple_sql_cache.clear()
                self._simple_sql_cache[sql] = cached_simple

            if cached_simple and cached_simple[0] == 'point':
                _, table_name, point_id, _ = cached_simple
                try:
                    self._ensure_table_selected()
                    if self._current_table and table_name.lower() == self._current_table.lower():
                        if show_internal_id is None:
                            show_internal_id = False
                        row = self._storage.retrieve(point_id)
                        if row is not None:
                            if not show_internal_id and '_id' in row:
                                row = {k: v for k, v in row.items() if k != '_id'}
                            rv = ResultView(data=[row])
                            rv._show_internal_id = show_internal_id
                            return rv
                        if self._pending_memtable_point_miss():
                            rv = ResultView(data=None)
                            rv._show_internal_id = show_internal_id
                            return rv
                except Exception:
                    pass  # fall through to the general SQL executor

            if cached_simple and cached_simple[0] == 'projected_point':
                _, table_name, point_id, columns = cached_simple
                try:
                    self._ensure_table_selected()
                    if self._current_table and table_name.lower() == self._current_table.lower():
                        row = self._storage.retrieve_projected_row(point_id, list(columns))
                        if row is None:
                            row = self._recover_projected_point_row(point_id, columns)
                        if row is not None:
                            rv = ResultView(data=[row])
                            rv._show_internal_id = show_internal_id if show_internal_id is not None else False
                            return rv
                except Exception:
                    pass  # fall through to the general SQL executor

            if cached_simple and cached_simple[0] == 'batch':
                _, table_name, ids, _ = cached_simple
                try:
                    self._ensure_table_selected()
                    if self._current_table and table_name.lower() == self._current_table.lower():
                        result = self._storage.retrieve_many(list(ids))
                        if result is not None:
                            columns_dict = result.get('columns_dict')
                            if columns_dict is not None:
                                rv = ResultView(lazy_pydict=columns_dict)
                                rv._show_internal_id = show_internal_id if show_internal_id is not None else False
                                return rv
                except Exception:
                    pass  # fall through to the general SQL executor

            if cached_simple and cached_simple[0] == 'projected_batch':
                _, table_name, ids, columns = cached_simple
                try:
                    self._ensure_table_selected()
                    if self._current_table and table_name.lower() == self._current_table.lower():
                        result = self._storage.retrieve_many_projected(list(ids), list(columns))
                        if result is not None:
                            columns_dict = result.get('columns_dict')
                            if columns_dict is not None:
                                rv = ResultView(lazy_pydict=columns_dict)
                                rv._show_internal_id = show_internal_id if show_internal_id is not None else False
                                return rv
                except Exception:
                    pass  # fall through to the general SQL executor

            if cached_simple and cached_simple[0] == 'string_eq_limit1':
                _, table_name, filter_col, filter_val, _ = cached_simple
                try:
                    self._ensure_table_selected()
                    if self._current_table and table_name.lower() == self._current_table.lower():
                        result = self._storage.retrieve_first_by_string_eq_limit1(filter_col, filter_val)
                        if result is not None:
                            columns_dict = result.get('columns_dict')
                            if columns_dict is not None:
                                rv = ResultView(lazy_pydict=columns_dict)
                                rv._show_internal_id = show_internal_id if show_internal_id is not None else False
                                return rv
                except Exception:
                    pass  # fall through to the general SQL executor

            if cached_simple and cached_simple[0] == 'projected_string_eq_limit1':
                _, table_name, filter_col, filter_val, columns = cached_simple
                try:
                    self._ensure_table_selected()
                    if self._current_table and table_name.lower() == self._current_table.lower():
                        result = self._storage.retrieve_projected_first_by_string_eq_limit1(
                            filter_col, filter_val, list(columns)
                        )
                        if result is not None:
                            columns_dict = result.get('columns_dict')
                            if columns_dict is not None:
                                rv = ResultView(lazy_pydict=columns_dict)
                                rv._show_internal_id = show_internal_id if show_internal_id is not None else False
                                return rv
                except Exception:
                    pass  # fall through to the general SQL executor

            if cached_simple and cached_simple[0] == 'projected_string_eq_limit':
                _, table_name, filter_col, filter_val, columns, limit_val, offset_val = cached_simple
                try:
                    self._ensure_table_selected()
                    if self._current_table and table_name.lower() == self._current_table.lower():
                        show_flag = show_internal_id if show_internal_id is not None else False
                        result = self._storage.retrieve_projected_by_string_eq_limit(
                            filter_col, filter_val, list(columns), limit_val, offset_val
                        )
                        if result is not None:
                            columns_dict = result.get('columns_dict')
                            if columns_dict is not None:
                                return self._result_view_from_columns_dict(
                                    sql,
                                    columns_dict,
                                    show_flag,
                                )
                except Exception:
                    pass  # fall through to the general SQL executor

        # ── Single-point classification (mirrors Rust QuerySignature) ──
        _sig, _count_star_match, _simple_projection = _classify_sql_route(
            sql, sql_upper
        )

        # ── Table selection check ──
        # Cross-db qualified refs (e.g. FROM default.users) don't need a selected table
        _qualified = _RE_QUALIFIED_REF.search(sql)
        _has_qualified_ref = bool(_qualified and '.' in _qualified.group(0))
        _references_view = self._references_persisted_view(sql)

        if _sig == 'table_func' or _sig == 'session':
            pass  # no table needed
        elif _sig == 'write':
            if not (sql_upper.startswith('CREATE ') or sql_upper.startswith('DROP TABLE')
                    or sql_upper.startswith('DROP VIEW')
                    or sql_upper.startswith('COPY ') or _has_qualified_ref or _references_view):
                self._ensure_table_selected()
        elif _sig == 'multi':
            try:
                self._ensure_table_selected()
            except Exception:
                pass
        elif _has_qualified_ref or sql_upper.startswith('WITH ') or _references_view:
            pass  # CTE or cross-db qualified refs don't need a selected table
        elif _sig in ('count_star', 'point_lookup', 'projected_point_lookup',
                      'batch_lookup', 'projected_batch_lookup', 'scan_limit',
                      'projected_scan_limit', 'projected_full_scan',
                      'projected_string_filter',
                      'projected_string_filter_limit', 'string_filter_limit',
                      'like', 'complex'):
            self._ensure_table_selected()

        # ── Determine locking ──
        _needs_lock = _sig in ('multi', 'write', 'transaction', 'session') or getattr(self, '_in_txn', False)

        with (self._lock if _needs_lock else _NULL_CONTEXT):
            if show_internal_id is None:
                show_internal_id = self._should_show_internal_id(sql)

            if (getattr(self, '_fast_txn_active', False)
                    and sql_upper.startswith('SELECT')):
                result = self._fast_txn_select(sql, show_internal_id)
                if result is not None:
                    return result

            if (not getattr(self, '_in_txn', False)
                    and _sig == 'write'
                    and sql_upper.startswith(('UPDATE', 'DELETE'))):
                self._flush_pending_overlay_writes_unlocked()

            # ── COUNT(*): ultra-fast atomic read ──
            if _sig == 'count_star':
                try:
                    count_alias = _count_star_match.group(1) if _count_star_match else None
                    count_table = _count_star_match.group(2) if _count_star_match else None
                    if count_table and '.' in count_table:
                        raise ValueError("qualified COUNT(*) uses the SQL executor")
                    if count_table and self._current_table and count_table.lower() != self._current_table.lower():
                        raise ValueError("non-current COUNT(*) table uses the SQL executor")
                    count = self._storage.fast_row_count()
                    rv = ResultView(lazy_pydict={count_alias or 'COUNT(*)': [count]})
                    rv._show_internal_id = False
                    return rv
                except Exception:
                    pass  # fall through to Arrow FFI

            # ── Point lookup: retrieve_rcix via execute() ──
            if _sig == 'point_lookup':
                point_id = self._extract_point_lookup_id(sql)
                if point_id is not None:
                    try:
                        row = self.retrieve(point_id)
                        if row is not None:
                            if not show_internal_id and '_id' in row:
                                row = {k: v for k, v in row.items() if k != '_id'}
                            rv = ResultView(data=[row])
                            rv._show_internal_id = show_internal_id
                            return rv
                    except Exception:
                        pass  # fall through to Rust execute()

            if _sig in (
                'point_lookup',
                'projected_point_lookup',
                'batch_lookup',
                'projected_batch_lookup',
                'projected_scan_limit',
                'projected_string_filter',
                'projected_string_filter_limit',
                'string_filter_limit',
            ):
                if _sig == 'projected_point_lookup':
                    try:
                        point_id = self._extract_point_lookup_id(sql)
                        table_name = _simple_from_table(sql)
                        if (point_id is not None and _simple_projection
                                and table_name and self._current_table
                                and table_name.lower() == self._current_table.lower()):
                            result = self._storage.retrieve_projected(point_id, _simple_projection)
                            if result is not None:
                                columns_dict = result.get('columns_dict')
                                if columns_dict is not None:
                                    if any(columns_dict.values()):
                                        return self._result_view_from_columns_dict(
                                            sql,
                                            columns_dict,
                                            show_internal_id,
                                        )
                                    row = self._recover_projected_point_row(
                                        point_id, _simple_projection
                                    )
                                    if row is not None:
                                        rv = ResultView(data=[row])
                                        rv._show_internal_id = show_internal_id
                                        return rv
                                    if self._pending_memtable_point_miss():
                                        rv = ResultView(data=None)
                                        rv._show_internal_id = show_internal_id
                                        return rv
                            if self._pending_memtable_point_miss():
                                rv = ResultView(data=None)
                                rv._show_internal_id = show_internal_id
                                return rv
                    except Exception:
                        pass  # fall through to Rust execute()
                elif _sig == 'projected_string_filter_limit':
                    try:
                        match = _RE_SIMPLE_STRING_EQ_LIMIT.search(sql)
                        table_name = _simple_from_table(sql)
                        if (match and _simple_projection and table_name and self._current_table
                                and table_name.lower() == self._current_table.lower()):
                            limit_val = int(match.group(3))
                            offset_val = int(match.group(4) or 0)
                            if limit_val == 1 and offset_val == 0:
                                result = self._storage.retrieve_projected_first_by_string_eq_limit1(
                                    match.group(1), match.group(2), _simple_projection
                                )
                                if result is not None:
                                    columns_dict = result.get('columns_dict')
                                    if columns_dict is not None:
                                        return self._result_view_from_columns_dict(
                                            sql,
                                            columns_dict,
                                            show_internal_id,
                                        )
                    except Exception:
                        pass  # fall through to Rust execute()
                elif _sig == 'string_filter_limit':
                    try:
                        match = _RE_SIMPLE_STRING_EQ_LIMIT.search(sql)
                        table_name = _simple_from_table(sql)
                        if (match and table_name and self._current_table
                                and table_name.lower() == self._current_table.lower()):
                            limit_val = int(match.group(3))
                            offset_val = int(match.group(4) or 0)
                            if limit_val == 1 and offset_val == 0:
                                result = self._storage.retrieve_first_by_string_eq_limit1(
                                    match.group(1), match.group(2)
                                )
                                if result is not None:
                                    columns_dict = result.get('columns_dict')
                                    if columns_dict is not None:
                                        return self._result_view_from_columns_dict(
                                            sql,
                                            columns_dict,
                                            show_internal_id,
                                        )
                    except Exception:
                        pass  # fall through to Rust execute()
                elif _sig == 'batch_lookup':
                    try:
                        ids = _simple_id_list(sql)
                        table_name = _simple_from_table(sql)
                        batch_ids = sorted(set(ids)) if ids else None
                        if (batch_ids and table_name and self._current_table
                                and table_name.lower() == self._current_table.lower()):
                            result = self._storage.retrieve_many(batch_ids)
                            if result is not None:
                                columns_dict = result.get('columns_dict')
                                if columns_dict is not None:
                                    return self._result_view_from_columns_dict(
                                        sql,
                                        columns_dict,
                                        show_internal_id,
                                    )
                    except Exception:
                        pass  # fall through to Rust execute()
                try:
                    result = self._storage.execute(sql)
                    if result is not None:
                        columns_dict = result.get('columns_dict')
                        if columns_dict is None and 'columns' in result and 'rows' in result:
                            cols = result['columns']
                            rows = result['rows']
                            if not rows:
                                rv = ResultView(data=None)
                                rv._show_internal_id = show_internal_id
                                return rv
                            columns_dict = {c: [row[i] for row in rows] for i, c in enumerate(cols)}
                        if columns_dict is not None:
                            return self._result_view_from_columns_dict(
                                sql,
                                columns_dict,
                                show_internal_id,
                            )
                except Exception:
                    pass  # fall through to Arrow FFI

            # ── Projected full scan: SELECT col1, col2 FROM table ──
            if _sig == 'projected_full_scan':
                try:
                    result = self._storage.execute(sql)
                    if isinstance(result, dict):
                        columns_dict = result.get('columns_dict')
                        if columns_dict is not None:
                            row_count = len(next(iter(columns_dict.values()), []))
                            if row_count == 0:
                                requested = _simple_projection_columns(sql)
                                if requested:
                                    available = set(self.list_fields())
                                    missing = [
                                        column for column in requested
                                        if column not in available
                                    ]
                                    if missing:
                                        raise KeyError(
                                            f"Projected column {missing[0]!r} does not exist"
                                        )
                                rv = ResultView(lazy_pydict=columns_dict)
                                rv._show_internal_id = show_internal_id
                                return rv
                            return self._result_view_from_columns_dict(
                                sql,
                                columns_dict,
                                show_internal_id,
                            )
                except KeyError as exc:
                    raise RuntimeError(str(exc)) from exc
                except Exception:
                    pass  # fall through to Arrow FFI

            # ── SELECT * LIMIT N: pread_rcix columnar via execute() ──
            if _sig == 'scan_limit':
                try:
                    limit_clause = sql_upper.rsplit('LIMIT', 1)[1].strip().rstrip(';')
                    limit_val = int(limit_clause.split()[0])
                except (ValueError, IndexError):
                    limit_val = 999999
                if limit_val <= 10000:
                    try:
                        result = self._storage.execute(sql)
                        if result is not None:
                            columns_dict = result.get('columns_dict')
                            if columns_dict is None and 'columns' in result and 'rows' in result:
                                cols = result['columns']
                                rows = result['rows']
                                columns_dict = {c: [row[i] for row in rows] for i, c in enumerate(cols)}
                            if columns_dict is not None:
                                return self._result_view_from_columns_dict(
                                    sql,
                                    columns_dict,
                                    show_internal_id,
                                )
                    except Exception:
                        pass  # fall through to Arrow FFI

            # ── Transaction commands ──
            if _sig == 'transaction':
                if getattr(self, '_fast_txn_active', False):
                    if sql_upper in ('COMMIT', 'COMMIT;'):
                        return self._commit_fast_txn(show_internal_id)
                    if sql_upper in ('ROLLBACK', 'ROLLBACK;'):
                        return self._rollback_fast_txn(show_internal_id)
                    self._promote_fast_txn_to_rust_unlocked()
                result = self._storage.execute(sql)
                if sql_upper.startswith('BEGIN'):
                    self._in_txn = True
                elif sql_upper in ('COMMIT', 'COMMIT;', 'ROLLBACK', 'ROLLBACK;'):
                    self._in_txn = False
                rv = ResultView(data=None)
                rv._show_internal_id = show_internal_id
                return rv

            # ── DML/SELECT within a transaction (single-statement only) ──
            if getattr(self, '_in_txn', False) and _sig != 'multi' and sql_upper.startswith(('INSERT', 'DELETE', 'UPDATE', 'SELECT')):
                if getattr(self, '_fast_txn_active', False):
                    if sql_upper.startswith('SELECT'):
                        result = self._fast_txn_select(sql, show_internal_id)
                        if result is not None:
                            return result
                        self._promote_fast_txn_to_rust_unlocked()
                    elif self._fast_txn_read_only:
                        self._promote_fast_txn_to_rust_unlocked("BEGIN TRANSACTION READ ONLY")
                    elif sql_upper.startswith('INSERT') and self._append_fast_txn_insert(sql):
                        return self._empty_sql_result(show_internal_id)
                    elif _RE_SIMPLE_NUMERIC_UPDATE_BY_ID.match(sql):
                        self._fast_txn_writes.append(("sql", sql))
                        return self._empty_sql_result(show_internal_id)
                    else:
                        self._promote_fast_txn_to_rust_unlocked()
                result = self._storage.execute(sql)
                if sql_upper.startswith('SELECT') and isinstance(result, dict):
                    # Prefer columns_dict (columnar, zero-copy from Rust)
                    columns_dict = result.get('columns_dict')
                    if columns_dict is not None:
                        return self._result_view_from_columns_dict(
                            sql,
                            columns_dict,
                            show_internal_id,
                        )
                    # Fallback: columns+rows format (transpose to columnar)
                    if 'columns' in result and 'rows' in result:
                        cols = result['columns']
                        rows = result['rows']
                        if cols and rows:
                            col_dict = {c: [row[i] for row in rows] for i, c in enumerate(cols)}
                            return self._result_view_from_columns_dict(
                                sql,
                                col_dict,
                                show_internal_id,
                            )
                rv = ResultView(data=None)
                rv._show_internal_id = show_internal_id
                return rv

            # ── Multi-statement: Arrow IPC with transaction support ──
            if _sig == 'multi':
                if getattr(self, '_fast_txn_active', False):
                    self._promote_fast_txn_to_rust_unlocked()
                ipc_bytes = self._storage._execute_arrow_ipc(sql)
                if 'BEGIN' in sql_upper or 'COMMIT' in sql_upper or 'ROLLBACK' in sql_upper:
                    for part in sql_upper.split(';'):
                        part = part.strip()
                        if part.startswith('BEGIN'):
                            self._in_txn = True
                        elif part in ('COMMIT', 'ROLLBACK') or part.startswith('COMMIT') or part == 'ROLLBACK':
                            self._in_txn = False
                if sql_upper.strip().rstrip(';').strip().startswith('CREATE TABLE'):
                    self._current_table = self._storage.current_table()
                if 'FTS INDEX' in sql_upper:
                    self._sync_fts_config_from_disk()
                pa_mod = _ensure_pyarrow()
                reader = pa_mod.ipc.open_stream(pa_mod.BufferReader(ipc_bytes))
                table = reader.read_all()
                rv = ResultView(arrow_table=table, data=None)
                rv._show_internal_id = show_internal_id
                return rv

            if (not getattr(self, '_in_txn', False)
                    and self._should_use_columnar_materialization(sql_upper, _sig)):
                try:
                    result = self._storage.execute(sql)
                    if isinstance(result, dict):
                        columns_dict = result.get('columns_dict')
                        if columns_dict is not None:
                            table_name = _simple_from_table(sql)
                            if (table_name and self._current_table
                                    and table_name.lower() == self._current_table.lower()):
                                cached_shape = self._simple_sql_cache.get(sql)
                                if not cached_shape or cached_shape[0] == 'columnar_select':
                                    if len(self._simple_sql_cache) >= 256:
                                        self._simple_sql_cache.clear()
                                    self._simple_sql_cache[sql] = ('columnar_select', table_name)
                            row_count = len(next(iter(columns_dict.values()), []))
                            if row_count == 0:
                                if not columns_dict:
                                    raise LookupError(
                                        "empty column metadata; use Arrow FFI"
                                    )
                                rv = ResultView(lazy_pydict=columns_dict)
                                rv._show_internal_id = show_internal_id
                                return rv
                            return self._result_view_from_columns_dict(
                                sql,
                                columns_dict,
                                show_internal_id,
                            )
                except LookupError:
                    pass  # Empty dict cannot preserve schema; use Arrow FFI.
                except Exception:
                    pass  # fall through to Arrow FFI

            # ── LIKE: zero-copy FFI scan ──
            if _sig == 'like' and not getattr(self, '_in_txn', False):
                try:
                    schema_ptr, array_ptr = self._storage._execute_like_ffi(sql)
                    if schema_ptr != 0 and array_ptr != 0:
                        pa_mod = _ensure_pyarrow()
                        batch = pa_mod.RecordBatch._import_from_c(array_ptr, schema_ptr)
                        table = pa_mod.Table.from_batches([batch])
                        rv = ResultView(arrow_table=table, data=None)
                        rv._show_internal_id = show_internal_id
                        return rv
                except Exception:
                    pass  # fall through to Arrow FFI

            # ── Validate table name for non-DDL queries ──
            if _sig not in ('write', 'table_func', 'session') or not sql_upper.startswith(('CREATE ', 'DROP TABLE')):
                if _sig == 'complex' or _sig == 'like':
                    self._validate_table_in_sql(sql)

            # Track write state
            if _sig == 'write':
                self._has_writes = True
                self._invalidate_replace_cache()

            # ── Default path: Arrow C Data Interface (zero-copy) ──
            try:
                schema_ptr, array_ptr = self._storage._execute_arrow_ffi(sql)
                if schema_ptr != 0 and array_ptr != 0:
                    pa_mod = _ensure_pyarrow()
                    batch = pa_mod.RecordBatch._import_from_c(array_ptr, schema_ptr)
                    table = pa_mod.Table.from_batches([batch])
                else:
                    table = None
            except Exception:
                # Fallback: Arrow IPC
                ipc_bytes = self._storage._execute_arrow_ipc(sql)
                pa_mod = _ensure_pyarrow()
                reader = pa_mod.ipc.open_stream(pa_mod.BufferReader(ipc_bytes))
                table = reader.read_all()

            # Sync Python state after DDL
            if sql_upper.startswith('CREATE TABLE'):
                self._current_table = self._storage.current_table()
            elif sql_upper.startswith('COPY '):
                import re as _re
                _m = _re.match(r'COPY\s+(\w+)\s+FROM\b', sql_upper)
                if _m:
                    self._current_table = _m.group(1).lower()
            if 'FTS INDEX' in sql_upper:
                self._sync_fts_config_from_disk()

            rv = ResultView(arrow_table=table, data=None)
            rv._show_internal_id = show_internal_id
            return rv

    def execute_batch(self, queries: List[str]) -> List['ResultView']:
        """Execute a SQL script in list order with read-after-write visibility.

        Args:
            queries: Statements to execute sequentially.

        Returns:
            One :class:`ResultView` per statement, in input order.
        """
        return [self.execute(query) for query in queries]

    def execute_batch_parallel(self, queries: List[str]) -> List['ResultView']:
        """Execute independent read-only statements concurrently.

        Statements that may mutate state are rejected so callers cannot
        accidentally depend on nondeterministic write ordering.
        """
        for query in queries:
            head = query.lstrip().split(None, 1)[0].upper() if query.strip() else ""
            if head not in {"SELECT", "WITH", "EXPLAIN"}:
                raise ValueError(
                    "execute_batch_parallel accepts independent read-only queries only; "
                    "use execute_batch for ordered scripts"
                )
        if not queries:
            return []
        if len(queries) == 1:
            return [self.execute(queries[0])]
        ipc_bytes_list = self._storage.execute_batch(queries)
        results = []
        for ipc_bytes in ipc_bytes_list:
            if ipc_bytes:
                pa_mod = _ensure_pyarrow()
                reader = pa_mod.ipc.open_stream(pa_mod.BufferReader(ipc_bytes))
                table = reader.read_all()
                rv = ResultView(arrow_table=table, data=None)
                results.append(rv)
            else:
                results.append(ResultView(arrow_table=None, data=None))
        return results

    def topk_distance(
        self,
        col: str,
        query,
        k: int = 10,
        metric: str = 'l2',
        id_col: str = '_id',
        dist_col: str = 'dist',
    ) -> 'ResultView':
        """Heap-based TopK vector distance search: O(n log k), faster than ORDER BY + LIMIT.

        Executes::

            SELECT explode_rename(topk_distance(col, [q], k, 'metric'), "id_col", "dist_col")
            FROM <current_table>

        Returns k rows with two columns: ``id_col`` (the ``_id`` values of the nearest
        rows) and ``dist_col`` (their distances), sorted ascending by distance.

        The result can be used directly or joined back to the original table::

            results = client.topk_distance('vec', query, k=10)
            # results has columns: _id, dist

        Args:
            col: Name of the binary vector column to search.
            query: Query vector — list, tuple, or numpy array of floats.
            k: Number of nearest neighbours to return (default 10).
            metric: Distance metric. Accepted values:
                ``'l2'`` / ``'euclidean'``,
                ``'l2_squared'``,
                ``'l1'`` / ``'manhattan'``,
                ``'linf'`` / ``'chebyshev'``,
                ``'cosine'`` / ``'cosine_distance'``,
                ``'dot'`` / ``'inner_product'``.
            id_col: Name for the output ``_id`` column (default ``'_id'``).
            dist_col: Name for the output distance column (default ``'dist'``).

        Returns:
            ResultView with ``id_col`` and ``dist_col`` columns, sorted nearest first.
        """
        self._check_connection()
        self._ensure_table_selected()
        if not isinstance(k, (int, np.integer)) or isinstance(k, (bool, np.bool_)) or k <= 0:
            raise ValueError("topk_distance: k must be a positive integer")
        query_array = np.asarray(query, dtype='<f4')
        if query_array.ndim != 1:
            raise ValueError("topk_distance: query must be a 1-D vector")
        if query_array.size == 0:
            raise ValueError("topk_distance: query must not be empty")
        if not np.isfinite(query_array).all():
            raise ValueError("topk_distance: query must contain only finite values")
        schema_ptr, array_ptr = self._storage._topk_distance_ffi(
            col, query_array.tobytes(), k, metric
        )
        if schema_ptr == 0 or array_ptr == 0:
            rv = ResultView(lazy_pydict={id_col: [], dist_col: []})
            rv._show_internal_id = True
            return rv
        pa_mod = _ensure_pyarrow()
        batch = pa_mod.RecordBatch._import_from_c(array_ptr, schema_ptr)
        table = pa_mod.Table.from_batches([batch])
        if table.column_names != [id_col, dist_col]:
            table = table.rename_columns([id_col, dist_col])
        rv = ResultView(arrow_table=table, data=None)
        rv._show_internal_id = True
        return rv

    def batch_topk_distance(
        self,
        col: str,
        queries,
        k: int = 10,
        metric: str = 'l2',
    ):
        """Batch heap-based TopK vector distance search — N queries in one Rust call.

        Significantly faster than calling ``topk_distance`` N times because:

        - The mmap float buffer (``scan_buf``) is loaded **once** regardless of N.
        - All N queries run in **parallel** via Rayon (outer parallelism over queries).
        - The ``_id`` column is read only once.

        Args:
            col:     Name of the vector column (FixedList or Binary).
            queries: ``(N, D)`` array-like or numpy array of query vectors (float32/float64).
            k:       Number of nearest neighbours per query (default 10).
            metric:  Distance metric — same values accepted as :meth:`topk_distance`.

        Returns:
            ``numpy.ndarray`` of shape ``(N, K, 2)``, dtype ``float64``, where

            - ``result[i, j, 0]``  is the ``_id`` of the j-th nearest neighbour for query i.
            - ``result[i, j, 1]``  is the corresponding distance.

            Each row is sorted ascending by distance.
            Entries padded with ``(-1, inf)`` when fewer than *k* neighbours exist.

        Example::

            queries = np.random.rand(100, 128).astype(np.float32)
            result = client.batch_topk_distance('vec', queries, k=10)
            # result.shape == (100, 10, 2)
            ids   = result[:, :, 0].astype(np.int64)   # (100, 10)
            dists = result[:, :, 1]                     # (100, 10)
        """
        import numpy as np
        self._check_connection()
        self._ensure_table_selected()
        queries = np.asarray(queries, dtype=np.float32)
        if queries.ndim == 1:
            queries = queries[np.newaxis, :]
        if queries.ndim != 2:
            raise ValueError("batch_topk_distance: queries must be a 2-D array of shape (N, D)")
        n, _d = queries.shape
        if n == 0 or _d == 0:
            raise ValueError("batch_topk_distance: queries must be non-empty")
        if not np.isfinite(queries).all():
            raise ValueError("batch_topk_distance: queries must contain only finite values")
        raw = self._storage._batch_topk_ffi(col, queries.tobytes(), n, k, metric)
        return np.frombuffer(raw, dtype=np.float64).reshape(n, k, 2)

    def _validate_table_in_sql(self, sql: str) -> None:
        """
        Validate that table names referenced in SQL exist (best-effort).

        Skips multi-statement DDL, CTEs, table functions, and qualified
        ``db.table`` references (resolved by the Rust executor).

        Args:
            sql: SQL statement text.

        Returns:
            None

        Raises:
            ValueError: If a referenced local table/view cannot be found.
        """
        # Skip validation for multi-statement SQL (contains CREATE TABLE/VIEW)
        if _RE_CREATE_TABLE.search(sql):
            return
        
        # Skip validation for CTE queries (WITH ... AS ...)
        if sql.strip().upper().startswith('WITH'):
            return
        
        # Extract table name from FROM clause
        m = _RE_FROM_TABLE.search(sql)
        if not m:
            return
        
        table_name = m.group(1).lower()

        # Skip validation for table functions: read_csv, read_json, read_parquet, topk_distance
        if table_name in ('read_csv', 'read_json', 'read_parquet', 'topk_distance'):
            return

        # Skip validation for qualified db.table names (e.g. "default.users", "analytics.events")
        # The Rust executor resolves cross-database paths; we cannot validate them here.
        if '.' in table_name:
            return
        
        # Fast path: skip expensive list_tables/listdir for known tables
        if self._current_table and table_name == self._current_table.lower():
            return
        
        # Check .apex file exists directly (O(1) vs O(n) listdir)
        apex_path = os.path.join(self._dirpath, f"{table_name}.apex")
        if os.path.exists(apex_path):
            return

        # Check temp table directory
        temp_apex_path = os.path.join(self._dirpath, ".apex_tmp", f"{table_name}.apex")
        if os.path.exists(temp_apex_path):
            return

        if self._relation_exists_as_view(table_name):
            return
        
        raise ValueError(f"Table '{m.group(1)}' not found")

    def _load_view_catalog(self) -> Dict[str, str]:
        """
        Load the persisted view catalog from ``.apex_views.json``.

        Returns:
            Mapping of lower-cased view name to catalog payload. Empty dict on
            missing/corrupt catalog.
        """
        try:
            try:
                stat = self._view_catalog_path.stat()
            except FileNotFoundError:
                self._view_catalog_known_present = False
                self._view_catalog_mtime_ns = None
                if self._view_catalog_views:
                    self._view_catalog_views = {}
                return {}
            mtime_ns = stat.st_mtime_ns
            if self._view_catalog_known_present and self._view_catalog_mtime_ns == mtime_ns:
                return self._view_catalog_views
            with open(self._view_catalog_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            views = payload.get("views", {})
            if isinstance(views, dict):
                self._view_catalog_views = {str(k).lower(): v for k, v in views.items()}
            else:
                self._view_catalog_views = {}
            self._view_catalog_known_present = True
            self._view_catalog_mtime_ns = mtime_ns
            return self._view_catalog_views
        except Exception:
            self._view_catalog_known_present = False
            self._view_catalog_mtime_ns = None
            self._view_catalog_views = {}
            return {}

    def _relation_exists_as_view(self, table_name: str) -> bool:
        """
        Return whether *table_name* exists as a persisted view.

        Args:
            table_name: Relation name to check.

        Returns:
            ``True`` if present in the view catalog.
        """
        return table_name.lower() in self._load_view_catalog()

    def _references_persisted_view(self, sql: str) -> bool:
        """
        Return whether SQL references any persisted view in FROM/JOIN clauses.

        Args:
            sql: SQL statement text.

        Returns:
            ``True`` if at least one local view relation is referenced.
        """
        for match in _RE_FROM_OR_JOIN_TABLE.finditer(sql):
            relation = match.group(1).lower()
            if relation in ('read_csv', 'read_json', 'read_parquet', 'topk_distance'):
                continue
            if '.' in relation:
                continue
            if self._relation_exists_as_view(relation):
                return True
        return False
    
    def _should_show_internal_id(self, sql: str) -> bool:
        """
        Determine whether ``_id`` should be visible based on the SELECT list.

        Args:
            sql: SQL statement text.

        Returns:
            ``True`` if ``_id`` is explicitly projected (and not only via ``*``).
        """
        # Fast path: if _id not mentioned at all, skip expensive regex
        if '_id' not in sql:
            return False
        
        # Check if _id is explicitly in SELECT clause
        m = _RE_SELECT_FROM.search(sql)
        if not m:
            return False
        
        select_list = m.group(1)
        
        # Check for explicit _id reference (not in aggregate functions)
        def has_explicit_id(item: str) -> bool:
            """
            Return whether a single SELECT-list item explicitly references ``_id``.

            Aggregate expressions are ignored so ``COUNT(_id)`` does not force
            ``_id`` visibility.

            Args:
                item: One comma-separated SELECT-list fragment.

            Returns:
                ``True`` if *item* explicitly names ``_id``.
            """
            s = item.strip()
            if _RE_AGGREGATE_FUNC.search(s):
                return False
            return bool(_RE_EXPLICIT_ID.search(s))
        
        # Split select items handling parentheses
        items = []
        buf = []
        depth = 0
        for ch in select_list:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            elif ch == ',' and depth == 0:
                items.append(''.join(buf).strip())
                buf = []
                continue
            buf.append(ch)
        if buf:
            items.append(''.join(buf).strip())
        
        has_star = any(re.fullmatch(r"\*", it.strip()) for it in items)
        has_id = any(has_explicit_id(it) for it in items)
        
        # Show _id if explicitly referenced (and not just SELECT *)
        if has_id and not (len(items) == 1 and has_star):
            return True
        return False

    def query(self, sql: str = None, where_clause: str = None, limit: int = None) -> 'ResultView':
        """
        Query with a full SQL statement or a WHERE-style filter expression.

        Compatibility helper around :meth:`execute`.

        Args:
            sql: Full ``SELECT``/``WITH`` statement, or a bare filter expression
                used as ``WHERE`` when it does not start with SELECT/WITH.
            where_clause: Alternative WHERE expression when *sql* is omitted.
            limit: Optional row limit appended to generated SELECT statements.

        Returns:
            :class:`ResultView` with matching rows.
        """
        self._ensure_table_selected()
        if sql is not None:
            # Check if it's a full SQL statement or a filter expression
            sql_upper = sql.strip().upper()
            if sql_upper.startswith("SELECT") or sql_upper.startswith("WITH"):
                # Full SQL statement
                return self.execute(sql)
            else:
                # Filter expression - convert to SELECT with WHERE
                full_sql = f"SELECT * FROM {self._current_table} WHERE {sql}"
                if limit:
                    full_sql += f" LIMIT {limit}"
                return self.execute(full_sql)
        elif where_clause is not None:
            full_sql = f"SELECT * FROM {self._current_table} WHERE {where_clause}"
            if limit:
                full_sql += f" LIMIT {limit}"
            return self.execute(full_sql)
        else:
            full_sql = f"SELECT * FROM {self._current_table}"
            if limit:
                full_sql += f" LIMIT {limit}"
            return self.execute(full_sql)

    def retrieve(self, id_: int) -> Optional[dict]:
        """
        Retrieve a single row by internal ``_id``.

        Args:
            id_: Row identifier.

        Returns:
            Row dict, or ``None`` if the ID does not exist.
        """
        self._check_connection()
        self._ensure_table_selected()
        return self._storage.retrieve(id_)

    def read_blob(self, column: str, id_: int) -> Optional[bytes]:
        """
        Read a BLOB payload by column name and ``_id``.

        Args:
            column: BLOB column name.
            id_: Row identifier.

        Returns:
            Payload bytes, or ``None`` if missing.
        """
        self._check_connection()
        self._ensure_table_selected()
        return self._storage.read_blob(column, id_)

    def read_blobs(self, column: str, ids: List[int]) -> List[Optional[bytes]]:
        """
        Read multiple BLOB payloads by column name and ``_id`` values.

        Args:
            column: BLOB column name.
            ids: List of row identifiers.

        Returns:
            List of payload bytes (or ``None`` per missing ID), aligned with *ids*.
        """
        self._check_connection()
        self._ensure_table_selected()
        return self._storage.read_blobs(column, ids)

    def read_blob_range(
        self,
        column: str,
        id_: int,
        offset: int = 0,
        length: Optional[int] = None,
    ) -> Optional[bytes]:
        """
        Read a byte range from a BLOB payload without materializing the whole value.

        Args:
            column: BLOB column name.
            id_: Row identifier.
            offset: Start offset in bytes (default ``0``).
            length: Number of bytes to read, or ``None`` for the remainder.

        Returns:
            Requested byte slice, or ``None`` if missing.
        """
        self._check_connection()
        self._ensure_table_selected()
        return self._storage.read_blob_range(column, id_, offset, length)

    def read_blob_ranges(
        self,
        column: str,
        ids: List[int],
        offsets: List[int],
        length: Optional[int] = None,
    ) -> List[Optional[bytes]]:
        """
        Read byte ranges from multiple BLOB payloads.

        Args:
            column: BLOB column name.
            ids: List of row identifiers.
            offsets: Per-ID start offsets (same length as *ids*).
            length: Shared read length, or ``None`` for remainder per blob.

        Returns:
            List of byte slices (or ``None``), aligned with *ids*.
        """
        self._check_connection()
        self._ensure_table_selected()
        return self._storage.read_blob_ranges(column, ids, offsets, length)

    def read_blob_descriptor(self, column: str, id_: int) -> Optional[bytes]:
        """
        Read the raw BLOB descriptor stored in the main ``.apex`` file.

        Args:
            column: BLOB column name.
            id_: Row identifier.

        Returns:
            Descriptor bytes, or ``None`` if missing.
        """
        self._check_connection()
        self._ensure_table_selected()
        return self._storage.read_blob_descriptor(column, id_)

    def read_blob_info(self, column: str, id_: int) -> Optional[dict]:
        """
        Read BLOB descriptor metadata without materializing the payload.

        Args:
            column: BLOB column name.
            id_: Row identifier.

        Returns:
            Metadata dict, or ``None`` if missing.
        """
        self._check_connection()
        self._ensure_table_selected()
        return self._storage.read_blob_info(column, id_)

    def read_blob_infos(self, column: str, ids: List[int]) -> List[Optional[dict]]:
        """
        Read BLOB descriptor metadata for multiple ``_id`` values.

        Args:
            column: BLOB column name.
            ids: List of row identifiers.

        Returns:
            List of metadata dicts (or ``None``), aligned with *ids*.
        """
        self._check_connection()
        self._ensure_table_selected()
        return self._storage.read_blob_infos(column, ids)

    def retrieve_many(self, ids: List[int]) -> 'ResultView':
        """
        Retrieve multiple rows by ``_id`` as a :class:`ResultView`.

        Args:
            ids: List of row identifiers.

        Returns:
            :class:`ResultView` containing the found rows (empty when *ids* is empty
            or nothing matches).
        """
        self._check_connection()
        self._ensure_table_selected()
        with self._lock:
            if not ids:
                return _empty_result_view()

            result = self._storage.retrieve_many(ids)
            columns_dict = result.get('columns_dict') if isinstance(result, dict) else None
            if columns_dict:
                return ResultView(lazy_pydict=columns_dict)
            return _empty_result_view()

    def retrieve_all(self) -> 'ResultView':
        """
        Retrieve all rows from the current table.

        Returns:
            :class:`ResultView` for ``SELECT *`` on the current table.
        """
        self._check_connection()
        self._ensure_table_selected()
        return self.execute(f"SELECT * FROM {self._current_table}")

    def list_fields(self) -> List[str]:
        """
        List column/field names of the current table.

        Returns:
            List of field name strings.
        """
        self._check_connection()
        self._ensure_table_selected()
        with self._lock:
            return self._storage.list_fields()

    # ============ Delete/Replace ============

    def delete(
        self, 
        id: Optional[Union[int, List[int]]] = None, 
        where: Optional[str] = None
    ) -> Union[bool, int]:
        """Delete records by ID(s) or WHERE clause.
        
        Args:
            id: Single ID (int) or list of IDs to delete. Optional.
            where: SQL WHERE clause string for conditional deletion. Optional.
                   Example: "age > 30" or "status = 'inactive'"
        
        Returns:
            - If deleting by id: bool indicating success
            - If deleting by where: int count of deleted rows
        
        Raises:
            ValueError: If neither id nor where is provided (safety protection)
        
        Examples:
            client.delete(id=1)                    # Delete single record
            client.delete(id=[1, 2, 3])            # Delete multiple records
            client.delete(where="age > 30")        # Delete matching records
        """
        self._check_connection()
        self._ensure_table_selected()
        
        # Safety check: require at least one parameter to prevent accidental deletion of all data
        if id is None and where is None:
            raise ValueError(
                "delete() requires at least one argument: 'id' or 'where'. "
                "To delete all records, use delete(where='1=1') explicitly."
            )

        fts_enabled = self._is_fts_enabled(self._current_table)
        missing_cache_key = None
        if where is None and isinstance(id, int) and not fts_enabled:
            missing_cache_key = (self._current_database, self._current_table, int(id))
            if self._last_missing_delete_key == missing_cache_key:
                return False

        with self._lock:
            # Case 1: Delete by WHERE clause
            if where is not None:
                # Note: FTS cleanup for WHERE-based delete would require 
                # querying IDs first, which is expensive. Skip for now.
                self._invalidate_replace_cache()
                return self._storage.delete_where(where)
            
            # Case 2: Delete by ID(s)
            if id is not None:
                if isinstance(id, int):
                    result = self._storage.delete(id)
                    if result:
                        if fts_enabled:
                            self._storage._fts_remove(id)
                        self._invalidate_replace_cache()
                    elif missing_cache_key is not None:
                        self._last_missing_delete_key = missing_cache_key
                    return result
                elif isinstance(id, list):
                    self._invalidate_replace_cache()
                    result = self._storage.delete_batch(id)
                    if result and fts_enabled:
                        for doc_id in id:
                            self._storage._fts_remove(doc_id)
                    return result
                else:
                    raise ValueError("id must be an int or a list of ints")

    def replace(self, id_: int, data: dict) -> bool:
        """
        Replace an existing row's fields by ``_id``.

        Updates the FTS index when enabled. Identical consecutive replaces are
        short-circuited via an exact-replace cache.

        Args:
            id_: Row identifier to replace.
            data: New field values (partial or full row dict, depending on storage).

        Returns:
            ``True`` if the row existed and was replaced; ``False`` otherwise.
        """
        self._check_connection()
        self._ensure_table_selected()
        with self._lock:
            cache_key = (self._current_database, self._current_table, int(id_))
            if self._last_exact_replace_key == cache_key and self._last_exact_replace_data == data:
                return True
            result = self._storage.replace(id_, data)
            if result:
                if self._is_fts_enabled(self._current_table):
                    self._ensure_fts_initialized(self._current_table)
                    content = self._extract_indexable_content(data)
                    self._storage._fts_index(id_, " ".join(content.values()))
                self._invalidate_replace_cache()
                self._remember_exact_replace(id_, data)
            elif self._last_exact_replace_key == cache_key:
                self._invalidate_replace_cache()
            return result

    def batch_replace(self, data_dict: Dict[int, dict]) -> List[int]:
        """
        Replace multiple rows by ``_id``.

        Args:
            data_dict: Mapping of row ``_id`` to replacement field dict.

        Returns:
            List of IDs that were successfully replaced.
        """
        self._check_connection()
        success_ids = []
        for id_, data in data_dict.items():
            if self.replace(id_, data):
                success_ids.append(id_)
        return success_ids

    # ============ DataFrame Import ============

    def from_pandas(self, df, table_name: str = None) -> 'ApexClient':
        """
        Import a pandas DataFrame into the current (or named) table.

        Args:
            df: ``pandas.DataFrame`` to store.
            table_name: Optional table to select/create before import.

        Returns:
            ``self`` for method chaining.
        """
        if table_name is not None:
            self._select_or_create_table(table_name)
        self._ensure_table_selected()
        self.store(df)
        return self

    def from_pyarrow(self, table, table_name: str = None) -> 'ApexClient':
        """
        Import a PyArrow Table into the current (or named) table.

        Args:
            table: ``pyarrow.Table`` to store.
            table_name: Optional table to select/create before import.

        Returns:
            ``self`` for method chaining.
        """
        if table_name is not None:
            self._select_or_create_table(
                table_name, self._arrow_schema_to_apex_schema(table.schema)
            )
        self._ensure_table_selected()
        self.store(table)
        return self

    @staticmethod
    def _arrow_table_to_apex_columns(table) -> Dict[str, list]:
        """Convert Arrow columns without losing temporal storage semantics."""
        pa_mod = _ensure_pyarrow()
        columns = {}
        for field in table.schema:
            column = table.column(field.name)
            typ = field.type
            if pa_mod.types.is_date32(typ):
                columns[field.name] = column.cast(pa_mod.int32()).to_pylist()
            elif pa_mod.types.is_date64(typ):
                millis = column.cast(pa_mod.int64()).to_pylist()
                columns[field.name] = [
                    None if value is None else value // 86_400_000
                    for value in millis
                ]
            elif pa_mod.types.is_timestamp(typ):
                raw = column.cast(pa_mod.int64()).to_pylist()
                unit = typ.unit
                if unit == "s":
                    columns[field.name] = [
                        None if value is None else value * 1_000_000 for value in raw
                    ]
                elif unit == "ms":
                    columns[field.name] = [
                        None if value is None else value * 1_000 for value in raw
                    ]
                elif unit == "ns":
                    columns[field.name] = [
                        None if value is None else value // 1_000 for value in raw
                    ]
                else:
                    columns[field.name] = raw
            else:
                columns[field.name] = column.to_pylist()
        return columns

    def from_lance(
        self,
        uri,
        table_name: str = None,
        columns: Optional[List[str]] = None,
        filter=None,
        limit: Optional[int] = None,
        batch_size: Optional[int] = None,
        **dataset_options,
    ) -> 'ApexClient':
        """
        Import a Lance dataset into the current or named ApexBase table.

        Data is read through Lance's Arrow path into ApexBase's columnar write path.

        Args:
            uri: Lance dataset URI/path, or an object exposing ``to_batches``.
            table_name: Optional table to select/create before import.
            columns: Optional column subset to read.
            filter: Optional Lance filter expression.
            limit: Optional max rows to import.
            batch_size: Optional Lance scan batch size.
            **dataset_options: Extra keyword args forwarded to ``lance.dataset``.

        Returns:
            ``self`` for method chaining.
        """
        lance_mod = _ensure_lance()
        dataset = uri if hasattr(uri, "to_batches") else lance_mod.dataset(uri, **dataset_options)

        schema = getattr(dataset, "schema", None)
        if table_name is not None:
            self._select_or_create_table(table_name, self._arrow_schema_to_apex_schema(schema))
        self._ensure_table_selected()

        table = dataset.to_table(
            columns=columns,
            filter=filter,
            limit=limit,
            batch_size=batch_size,
        )
        if table.num_rows:
            self.store(table)
        elif table_name is not None:
            self.flush()
        return self

    def from_polars(self, df, table_name: str = None) -> 'ApexClient':
        """
        Import a polars DataFrame into the current (or named) table.

        Args:
            df: ``polars.DataFrame`` to store.
            table_name: Optional table to select/create before import.

        Returns:
            ``self`` for method chaining.
        """
        if table_name is not None:
            self._select_or_create_table(table_name)
        self._ensure_table_selected()
        self.store(df)
        return self

    def to_lance(
        self,
        uri,
        sql: str = None,
        mode: str = "create",
        show_internal_id: bool = False,
        **write_options,
    ):
        """
        Export the current table or a SQL result to a Lance dataset.

        Args:
            uri: Destination Lance dataset URI/path.
            sql: Optional SQL whose result is exported instead of the full table.
                When omitted, exports ``SELECT *`` from the current table.
            mode: Lance write mode (default ``"create"``).
            show_internal_id: Whether to include ``_id`` when exporting a SQL result
                (default ``False``).
            **write_options: Extra keyword args forwarded to the Lance writer.

        Returns:
            Result of ``ResultView.to_lance`` (typically the written dataset URI/path).
        """
        if sql is None:
            self._ensure_table_selected()
            result = self.query()
        else:
            result = self.execute(sql, show_internal_id=show_internal_id)
        return result.to_lance(uri, mode=mode, **write_options)

    def _select_or_create_table(self, table_name: str, schema: dict = None):
        """
        Select an existing table or create a new one, then make it current.

        Args:
            table_name: Table name to select/create.
            schema: Optional schema dict used when creating a new table.

        Returns:
            None
        """
        try:
            self.use_table(table_name)
        except (ValueError, RuntimeError):
            self.create_table(table_name, schema)

    @staticmethod
    def _arrow_schema_to_apex_schema(schema) -> Optional[dict]:
        """
        Map a PyArrow schema to an ApexBase column-type schema dict.

        Args:
            schema: ``pyarrow.Schema`` (or compatible) object.

        Returns:
            Mapping of column name to ApexBase type string, or ``None`` if empty.
        """
        if schema is None:
            return None
        pa_mod = _ensure_pyarrow()
        mapped = {}
        for field in schema:
            typ = field.type
            if pa_mod.types.is_int8(typ):
                mapped[field.name] = "int8"
            elif pa_mod.types.is_int16(typ):
                mapped[field.name] = "int16"
            elif pa_mod.types.is_int32(typ):
                mapped[field.name] = "int32"
            elif pa_mod.types.is_int64(typ):
                mapped[field.name] = "int64"
            elif pa_mod.types.is_uint8(typ):
                mapped[field.name] = "uint8"
            elif pa_mod.types.is_uint16(typ):
                mapped[field.name] = "uint16"
            elif pa_mod.types.is_uint32(typ):
                mapped[field.name] = "uint32"
            elif pa_mod.types.is_uint64(typ):
                mapped[field.name] = "uint64"
            elif pa_mod.types.is_float32(typ):
                mapped[field.name] = "float32"
            elif pa_mod.types.is_float64(typ):
                mapped[field.name] = "float64"
            elif pa_mod.types.is_boolean(typ):
                mapped[field.name] = "bool"
            elif pa_mod.types.is_binary(typ):
                mapped[field.name] = "binary"
            elif pa_mod.types.is_large_binary(typ):
                mapped[field.name] = "blob"
            elif pa_mod.types.is_fixed_size_list(typ) and (
                pa_mod.types.is_float16(typ.value_type)
                or pa_mod.types.is_float32(typ.value_type)
                or pa_mod.types.is_float64(typ.value_type)
            ):
                mapped[field.name] = "float16_vector"
            elif pa_mod.types.is_date32(typ) or pa_mod.types.is_date64(typ):
                mapped[field.name] = "date"
            elif pa_mod.types.is_timestamp(typ):
                mapped[field.name] = "timestamp"
            else:
                mapped[field.name] = "string"
        return mapped

    # ============ Utility ============

    def optimize(self):
        """
        Best-effort optimize hook (currently flushes pending writes).

        Returns:
            None
        """
        self._check_connection()
        # ApexStorage doesn't have optimize, just flush
        self.flush()

    def count_rows(self, table_name: str = None) -> int:
        """
        Return the row count of a table.

        Args:
            table_name: Optional table name. Defaults to the current table.
                Temporarily switches tables when a different name is provided.

        Returns:
            Integer row count.
        """
        self._check_connection()
        with self._lock:
            if table_name and table_name != self._current_table:
                original = self._current_table
                self.use_table(table_name)
                count = self._storage.row_count()
                if original is not None:
                    self.use_table(original)
                return count
            self._ensure_table_selected()
            return self._storage.row_count()

    def flush(self) -> None:
        """
        Flush buffered client writes and storage-level pending data to disk.

        No-ops when there are no pending writes/overlays.

        Returns:
            None
        """
        self._check_connection()
        with self._lock:
            if (not self._has_writes
                    and not self._buffered_write_rows
                    and not getattr(self._storage, "has_pending_overlay_writes", lambda: False)()):
                return
            self.flush_buffered_writes()
            self._storage.flush()
            self._has_writes = False

    def begin_buffered_writes(self, flush_rows: int = 0) -> None:
        """
        Enable explicit client-local buffered single-row writes.

        Rows become durable/visible after :meth:`flush_buffered_writes`,
        :meth:`flush`, or :meth:`close`. Trades immediate visibility for lower
        per-row Python overhead in OLTP-style append bursts.

        Args:
            flush_rows: Auto-flush after this many buffered rows (``0`` disables).

        Returns:
            None
        """
        self._check_connection()
        with self._lock:
            self._ensure_table_selected()
            self._buffered_writes_enabled = True
            self._buffered_write_table = self._current_table
            self._buffered_write_flush_rows = max(0, int(flush_rows or 0))

    def end_buffered_writes(self, flush: bool = True) -> None:
        """
        Disable buffered writes, optionally flushing pending rows first.

        Args:
            flush: If ``True`` (default), flush pending rows; otherwise discard them.

        Returns:
            None
        """
        self._check_connection()
        with self._lock:
            if flush:
                self.flush_buffered_writes()
            else:
                self._buffered_write_rows.clear()
                self._buffered_write_table = None
            self._buffered_writes_enabled = False
            self._buffered_write_flush_rows = 0

    def flush_buffered_writes(self) -> int:
        """
        Flush pending buffered single-row writes.

        Returns:
            Number of rows flushed.
        """
        self._check_connection()
        with self._lock:
            return self._flush_buffered_writes_unlocked()

    def _flush_buffered_writes_unlocked(self) -> int:
        """
        Flush buffered writes while the caller already holds ``self._lock``.

        Returns:
            Number of rows flushed.
        """
        if not self._buffered_write_rows:
            return 0
        table = self._buffered_write_table or self._current_table
        rows = self._buffered_write_rows

        old_enabled = self._buffered_writes_enabled
        self._buffered_writes_enabled = False
        original_table = self._current_table
        try:
            if table and table != self._current_table:
                self._storage.use_table(table)
                self._current_table = table
            if len(rows) == 1:
                self._storage.store(rows[0])
            else:
                self._store_batch_optimized(rows)
            self._buffered_write_rows = []
            self._buffered_write_table = None
            self._has_writes = True
            self._invalidate_replace_cache()
            return len(rows)
        finally:
            if original_table and original_table != self._current_table:
                self._storage.use_table(original_table)
                self._current_table = original_table
            self._buffered_writes_enabled = old_enabled

    def buffered_write_count(self) -> int:
        """
        Return the number of pending client-local buffered rows.

        Returns:
            Pending buffered row count.
        """
        return len(getattr(self, "_buffered_write_rows", []))
    
    def flush_cache(self):
        """
        Alias for :meth:`flush` (legacy name).

        Returns:
            None
        """
        self.flush()
    
    def set_auto_flush(self, rows: int = 0, bytes: int = 0) -> None:
        """
        Set storage auto-flush thresholds.

        When either threshold is exceeded during writes, data is automatically
        written to file. Set to ``0`` to disable the respective threshold.

        Args:
            rows: Auto-flush when pending rows exceed this count (``0`` = disabled).
            bytes: Auto-flush when estimated memory exceeds this size (``0`` = disabled).

        Returns:
            None
        """
        self._check_connection()
        with self._lock:
            self._storage.set_auto_flush(rows=rows, bytes=bytes)
    
    def get_auto_flush(self) -> tuple:
        """
        Get current auto-flush configuration.

        Returns:
            Tuple ``(rows_threshold, bytes_threshold)``.
        """
        self._check_connection()
        with self._lock:
            return self._storage.get_auto_flush()
    
    def estimate_memory_bytes(self) -> int:
        """
        Get estimated in-memory usage of pending storage buffers.

        Returns:
            Estimated memory usage in bytes.
        """
        self._check_connection()
        with self._lock:
            return self._storage.estimate_memory_bytes()

    # ============ Column Operations ============

    def drop_column(self, column_name: str):
        """
        Drop a column from the current table.

        Args:
            column_name: Column to drop. Cannot be ``_id``.

        Returns:
            None

        Raises:
            ValueError: If attempting to drop ``_id``.
        """
        self._check_connection()
        if column_name == '_id':
            raise ValueError("Cannot drop _id column")
        self._invalidate_replace_cache()
        self._storage.drop_column(column_name)

    def add_column(self, column_name: str, column_type: str):
        """
        Add a column to the current table.

        Args:
            column_name: New column name.
            column_type: ApexBase type string (e.g. ``"string"``, ``"int64"``).

        Returns:
            None
        """
        self._check_connection()
        self._invalidate_replace_cache()
        self._storage.add_column(column_name, column_type)

    def rename_column(self, old_column_name: str, new_column_name: str):
        """
        Rename a column on the current table.

        Args:
            old_column_name: Existing column name. Cannot be ``_id``.
            new_column_name: New column name.

        Returns:
            None

        Raises:
            ValueError: If attempting to rename ``_id``.
        """
        self._check_connection()
        if old_column_name == '_id':
            raise ValueError("Cannot rename _id column")
        if new_column_name in self.list_fields():
            raise ValueError(f"Column '{new_column_name}' already exists")
        self._invalidate_replace_cache()
        self._storage.rename_column(old_column_name, new_column_name)

    def get_column_dtype(self, column_name: str) -> str:
        """
        Return the storage type string for a column.

        Args:
            column_name: Column name.

        Returns:
            Type string such as ``"int64"`` or ``"string"``.
        """
        self._check_connection()
        return self._storage.get_column_dtype(column_name)

    # ============ FTS Search ==========

    def search_text(self, query: str, table_name: str = None) -> Optional[np.ndarray]:
        """
        Full-text search returning matching document IDs.

        Args:
            query: Search query text.
            table_name: Table to search. Defaults to the current table.

        Returns:
            ``numpy.ndarray`` of ``int64`` document IDs (possibly empty).

        Raises:
            ValueError: If FTS is not enabled for the table.
        """
        self._check_connection()
        table = table_name or self._current_table
        
        if not self._is_fts_enabled(table):
            raise ValueError(f"Full-text search is not enabled for table '{table}'. Call init_fts() first.")

        with self._fts_table_context(table):
            if not self._ensure_fts_initialized(table):
                return np.array([], dtype=np.int64)
            results = self._storage.search_text(query, limit=1000)
        if results is None:
            return np.array([], dtype=np.int64)
        if not results:
            return np.array([], dtype=np.int64)
        
        return np.array([r[0] for r in results], dtype=np.int64)

    def search_text_with_scores(
        self,
        query: str,
        table_name: str = None,
        limit: int = 1000,
        fuzzy: bool = False,
        min_results: int = 1,
    ) -> List[Tuple[int, float]]:
        """
        Full-text search returning ranked internal ``(_id, score)`` pairs.

        Args:
            query: Search query text.
            table_name: Table to search. Defaults to the current table.
            limit: Maximum number of hits (default ``1000``).
            fuzzy: Relax term matching when exact search is insufficient.
            min_results: Soft minimum used while relaxing fuzzy matching.

        Returns:
            List of internal ``(_id, score)`` tuples sorted by relevance.

        Raises:
            ValueError: If FTS is not enabled for the table.
        """
        self._check_connection()
        table = table_name or self._current_table
        if not self._is_fts_enabled(table):
            raise ValueError(f"Full-text search is not enabled for table '{table}'. Call init_fts() first.")

        with self._fts_table_context(table):
            if not self._ensure_fts_initialized(table):
                return []
            if fuzzy:
                results = self._storage.fuzzy_search_text(
                    query,
                    limit=max(0, limit),
                    min_results=max(0, min_results),
                )
            else:
                results = self._storage.search_text(query, limit=max(0, limit))
        return [(int(doc_id), float(score)) for doc_id, score in (results or [])]

    def fuzzy_search_text(self, query: str, min_results: int = 1, table_name: str = None) -> Optional[np.ndarray]:
        """
        Fuzzy full-text search returning matching internal ``_id`` values.

        Args:
            query: Search query text.
            min_results: Soft minimum results before relaxing matching (default ``1``).
            table_name: Table to search. Defaults to the current table.

        Returns:
            ``numpy.ndarray`` of internal ``_id`` values (possibly empty).

        Raises:
            ValueError: If FTS is not enabled for the table.
        """
        self._check_connection()
        table = table_name or self._current_table
        
        if not self._is_fts_enabled(table):
            raise ValueError(f"Full-text search is not enabled for table '{table}'. Call init_fts() first.")

        with self._fts_table_context(table):
            if not self._ensure_fts_initialized(table):
                return np.array([], dtype=np.int64)
            results = self._storage.fuzzy_search_text(query, limit=1000, min_results=min_results)
        if not results:
            return np.array([], dtype=np.int64)
        
        return np.array([r[0] for r in results], dtype=np.int64)

    def search_and_retrieve(self, query: str, table_name: str = None,
                           limit: Optional[int] = None, offset: int = 0) -> 'ResultView':
        """
        Full-text search and retrieve matching rows as a :class:`ResultView`.

        Args:
            query: Search query text.
            table_name: Table to search. Defaults to the current table.
            limit: Optional maximum number of rows.
            offset: Number of hits to skip (default ``0``).

        Returns:
            :class:`ResultView` of matching rows (empty if none).

        Raises:
            ValueError: If FTS is not enabled for the table.
        """
        self._check_connection()
        target_table = table_name or self._current_table

        if not self._is_fts_enabled(target_table):
            raise ValueError(f"Full-text search is not enabled for table '{target_table}'. Call init_fts() first.")

        with self._fts_table_context(target_table):
            if not self._ensure_fts_initialized(target_table):
                return _empty_result_view()
            # Default path: dict format - fastest for typical use cases
            result = self._storage.search_and_retrieve(query, limit=limit, offset=offset)
            columns_dict = result.get('columns_dict') if isinstance(result, dict) else None
            if columns_dict:
                return ResultView(lazy_pydict=columns_dict)
            return _empty_result_view()

    def search_and_retrieve_top(self, query: str, n: int = 100, table_name: str = None) -> 'ResultView':
        """
        Retrieve the top-*n* FTS hits for a query.

        Args:
            query: Search query text.
            n: Maximum number of rows (default ``100``).
            table_name: Table to search. Defaults to the current table.

        Returns:
            :class:`ResultView` of top matching rows.
        """
        self._check_connection()
        return self.search_and_retrieve(query, table_name=table_name, limit=n, offset=0)

    def set_fts_fuzzy_config(self, threshold: float = 0.7, max_distance: int = 2, 
                             max_candidates: int = 20, table_name: str = None):
        """
        Configure fuzzy matching parameters for the table's FTS engine.

        Args:
            threshold: Similarity threshold in ``[0, 1]`` (default ``0.7``).
            max_distance: Maximum edit distance (default ``2``).
            max_candidates: Maximum fuzzy candidate terms (default ``20``).
            table_name: Target table. Defaults to the current table.

        Returns:
            None

        Raises:
            ValueError: If FTS is not enabled/initialized for the table.
        """
        self._check_connection()
        table = table_name or self._current_table
        with self._fts_table_context(table):
            if not self._ensure_fts_initialized(table):
                raise ValueError(f"Full-text search is not enabled for table '{table}'.")
            self._storage._fts_set_fuzzy_config(threshold, max_distance, max_candidates)

    def get_fts_stats(self, table_name: str = None) -> Dict:
        """
        Return FTS status and basic index statistics for a table.

        Args:
            table_name: Target table. Defaults to the current table.

        Returns:
            Dict including at least ``fts_enabled``. When initialized, also
            includes ``engine_initialized``, ``doc_count``, and ``term_count``.
        """
        self._check_connection()
        table = table_name or self._current_table
        
        if not self._is_fts_enabled(table):
            return {'fts_enabled': False, 'table': table}
        
        with self._fts_table_context(table):
            if not self._ensure_fts_initialized(table):
                return {'fts_enabled': True, 'engine_initialized': False, 'table': table}
            stats = self._storage.get_fts_stats()
        if stats:
            return {
                'fts_enabled': True,
                'engine_initialized': True,
                'doc_count': stats[0],
                'term_count': stats[1]
            }
        return {'fts_enabled': True, 'engine_initialized': False, 'table': table}

    def compact_fts_index(self, table_name: str = None):
        """
        Compact the FTS index for a table.

        Args:
            table_name: Target table. Defaults to the current table.

        Returns:
            None

        Raises:
            ValueError: If FTS is not enabled/initialized for the table.
        """
        self._check_connection()
        table = table_name or self._current_table
        with self._fts_table_context(table):
            if not self._ensure_fts_initialized(table):
                raise ValueError(f"Full-text search is not enabled for table '{table}'.")
            self._storage._fts_compact()

    def warmup_fts_terms(self, terms: List[str], table_name: str = None) -> int:
        """
        Warm FTS caches for the given terms.

        Args:
            terms: List of terms to preload.
            table_name: Target table. Defaults to the current table.

        Returns:
            Number of terms warmed, or ``0`` if FTS is not initialized.
        """
        self._check_connection()
        table = table_name or self._current_table
        with self._fts_table_context(table):
            if not self._ensure_fts_initialized(table):
                return 0
            return self._storage._fts_warmup(terms)

    # ============ Lifecycle ============

    def _force_close(self):
        """
        Best-effort close used by finalizers; swallows close failures.

        Returns:
            None
        """
        try:
            self.close()
        except Exception:
            try:
                if hasattr(self, '_storage') and self._storage is not None:
                    self._storage = None
            except Exception:
                pass
            self._is_closed = True

    def close(self):
        """
        Close the client, flush pending work, and release shared storage if last user.

        Safe to call multiple times. After close, further operations raise
        ``RuntimeError``.

        Returns:
            None
        """
        if self._is_closed:
            return

        storage_lock = getattr(self, '_storage_lock', None)
        storage_context = storage_lock if storage_lock is not None else _NULL_CONTEXT
        storage_to_close = None
        with storage_context:
            if self._is_closed:
                return
            try:
                if hasattr(self, '_storage') and self._storage is not None:
                    try:
                        self.flush_buffered_writes()
                    except Exception:
                        pass
                    try:
                        self._flush_pending_memtable_rows_for_read()
                    except Exception:
                        pass
                    # Best-effort: ensure FTS index is persisted across reopen
                    try:
                        if any((isinstance(v, dict) and v.get('enabled', False)) for v in self._fts_tables.values()):
                            self._storage._fts_flush()
                    except Exception:
                        pass
            finally:
                self._is_closed = True
                current_storage = getattr(self, '_storage', None)
                self._storage = None
                if self._auto_manage:
                    client_id = getattr(self, '_client_id', None)
                    storage_to_close = _registry.unregister(str(self._db_path), client_id)
                else:
                    storage_to_close = current_storage

        if storage_to_close is not None:
            try:
                storage_to_close.close()
            except Exception:
                pass

    @classmethod
    def create_clean(cls, dirpath=None, **kwargs):
        """
        Construct a client that recreates storage under *dirpath* (``drop_if_exists=True``).

        Args:
            dirpath: Database directory path.
            **kwargs: Additional :class:`ApexClient` constructor arguments.

        Returns:
            A new :class:`ApexClient` instance.
        """
        kwargs['drop_if_exists'] = True
        return cls(dirpath=dirpath, **kwargs)

    def __enter__(self):
        """
        Enter the context manager.

        Returns:
            ``self``.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context manager and close the client.

        Args:
            exc_type: Exception type, if any.
            exc_val: Exception instance, if any.
            exc_tb: Traceback, if any.

        Returns:
            ``False`` so exceptions are not suppressed.
        """
        self.close()
        return False

    def __del__(self):
        """
        Finalizer that force-closes the client if still open.

        Returns:
            None
        """
        if hasattr(self, '_is_closed') and not self._is_closed:
            self._force_close()

    def __repr__(self):
        """
        Return a concise debug representation of the client.

        Returns:
            String such as ``ApexClient(path='...', table='...')``.
        """
        return f"ApexClient(path='{self._dirpath}', table='{self._current_table}')"
