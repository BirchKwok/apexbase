"""Chunked row-group layout: window reads, format declaration and layout fixes.

The chunked layout is the **default** for compressed row groups
(``APEX_CHUNK_INDEX=0`` opts out). These tests run in subprocesses so the
process-wide switch cannot leak into other tests. They pin three behaviours:

* window reads (``LIMIT``/``OFFSET``/projection) return exactly the rows a flat
  table returns, repeated reads stay identical, and a delete survives;
* a file that stores chunked groups declares format version 5 even without a
  narrow ``Float32`` column, so a V4-era build refuses it at open instead of
  meeting an unknown row-group revision;
* a full-file rewrite (the compressed-row-group DELETE path) preserves the
  source table's layout and version instead of switching on whatever the
  process-level opt-in happens to be.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap

ROWS = 120_000

BUILD = textwrap.dedent(
    f"""
    import json
    import os
    import tempfile

    import numpy as np

    from apexbase.client import ApexClient

    rows = {ROWS}
    tmp = tempfile.mkdtemp(prefix="apex_chunked_layout_")
    rng = np.random.default_rng(4242)
    k = rng.integers(0, 1_000_000, rows).tolist()
    x = rng.random(rows)
    s = [f"v{{int(v) % 100:03d}}" for v in rng.integers(0, 1_000_000, rows)]

    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.create_table("t", {{"k": "int64", "x": "float64", "s": "string"}})
    client.use_table("t")
    client.set_compression("zstd")
    client.store({{"k": k, "x": x.tolist(), "s": s}})
    client.flush()
    client.close()

    path = os.path.join(tmp, "t.apex")
    with open(path, "rb") as handle:
        head = handle.read(256 + 32)
    version = int.from_bytes(head[8:12], "little")
    encoding = head[256 + 29]
    with open(os.path.join(tmp, "meta.json"), "w") as handle:
        json.dump({{"version": version, "encoding": encoding}}, handle)
    print(json.dumps({{"tmp": tmp, "version": version, "encoding": encoding}}))
    """
)

READS = textwrap.dedent(
    f"""
    import json
    import os
    import sys

    import numpy as np

    from apexbase.client import ApexClient

    tmp = sys.argv[1]
    rows = {ROWS}
    rng = np.random.default_rng(4242)
    k = rng.integers(0, 1_000_000, rows).tolist()
    x = rng.random(rows)
    s = [f"v{{int(v) % 100:03d}}" for v in rng.integers(0, 1_000_000, rows)]
    mid = rows // 2 + 1

    def sig(table):
        client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
        client.use_table(table)
        out = {{}}
        out["limit"] = client.execute(
            "SELECT k FROM t LIMIT 100"
        ).to_dict()
        out["limit_again"] = client.execute(
            "SELECT k FROM t LIMIT 100"
        ).to_dict()
        out["offset"] = client.execute(
            "SELECT k, x FROM t LIMIT 10 OFFSET 1"
        ).to_dict()
        out["star_limit"] = client.execute(
            "SELECT * FROM t LIMIT 25"
        ).to_dict()
        out["count"] = client.execute(
            "SELECT COUNT(x) AS n FROM t WHERE k >= 1000"
        ).to_dict()[0]["n"]
        out["sum"] = client.execute(
            "SELECT SUM(x) AS s FROM t WHERE k >= 1000"
        ).to_dict()[0]["s"]
        out["string_count"] = client.execute(
            "SELECT COUNT(*) AS n FROM t WHERE s = 'v007'"
        ).to_dict()[0]["n"]
        out["point"] = client.execute(
            "SELECT k FROM t WHERE _id = " + str(mid)
        ).to_dict()
        out["total"] = client.execute(
            "SELECT COUNT(*) AS n FROM t"
        ).to_dict()[0]["n"]
        client.close()
        return out

    out = sig("t")
    assert len(out["limit"]) == 100, out["limit"]
    assert [row["k"] for row in out["limit"]] == k[:100], "LIMIT must return the first rows"
    assert out["limit"] == out["limit_again"], "repeated window reads must be identical"
    assert len(out["offset"]) == 10, out["offset"]
    assert [row["k"] for row in out["offset"]] == k[1:11], "OFFSET must skip exactly one row"
    assert len(out["star_limit"]) == 25, out["star_limit"]
    assert out["count"] == sum(1 for value in k if value >= 1000), out["count"]
    expected_sum = float(np.sum([value for value, key in zip(x, k) if key >= 1000]))
    assert abs(out["sum"] - expected_sum) <= abs(expected_sum) * 1e-9 + 1e-6, out["sum"]
    assert out["string_count"] == sum(1 for value in s if value == "v007"), out["string_count"]
    assert out["point"] == [{{"k": k[mid - 1]}}], (out["point"], k[mid - 1])
    assert out["total"] == rows, out["total"]

    # The compressed-row-group DELETE path rewrites the base file; reads after it
    # must stay correct and the layout must not change.
    client = ApexClient(tmp, drop_if_exists=False, enable_cache=False)
    client.use_table("t")
    client.execute("DELETE FROM t WHERE k < 1000")
    expected_removed = sum(1 for value in k if value < 1000)
    out["after_delete"] = client.execute("SELECT COUNT(*) AS n FROM t").to_dict()[0]["n"]
    assert out["after_delete"] == rows - expected_removed, (
        out["after_delete"],
        rows - expected_removed,
    )
    out["after_limit"] = client.execute("SELECT k FROM t LIMIT 100").to_dict()
    assert [row["k"] for row in out["after_limit"]] == [
        value for value in k if value >= 1000
    ][:100], "window reads after the rewrite must skip deleted rows"
    client.close()
    print(json.dumps(out))
    """
)


def _run(args, env_overrides, timeout=600):
    env = dict(os.environ)
    for key, value in env_overrides.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    result = subprocess.run(
        [sys.executable, *args],
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def test_default_chunked_window_reads_and_v5_declaration():
    build = _run(["-c", BUILD], {"APEX_CHUNK_INDEX": None})
    import json

    info = json.loads(build.stdout.strip().splitlines()[-1])
    assert info["encoding"] == 2, "independent columns must default to per-column frames"
    assert info["version"] == 5, (
        "a chunked group is a V5-only revision even without a Float32 column"
    )
    reads = _run(["-c", READS, info["tmp"]], {"APEX_CHUNK_INDEX": None})
    assert "after_delete" in reads.stdout


def test_opt_out_keeps_the_flat_layout():
    for value in ("0", "false", "no", "off"):
        build = _run(["-c", BUILD], {"APEX_CHUNK_INDEX": value})
        import json

        info = json.loads(build.stdout.strip().splitlines()[-1])
        assert info["encoding"] == 1, f"APEX_CHUNK_INDEX={value} must keep flat frames"
        assert info["version"] == 4, (
            "the opt-out keeps the V4 declaration for a flat zstd table"
        )
        reads = _run(["-c", READS, info["tmp"]], {"APEX_CHUNK_INDEX": value})
        assert "after_delete" in reads.stdout


def test_rewrite_preserves_the_source_layout():
    # A full-file rewrite (compressed-RG DELETE) must keep the table's layout:
    # creating one layout and rewriting under the other opt-in used to be able to
    # convert the file, which changes what older builds can read.
    import json

    for create_env, rewrite_env, encoding in (
        ("0", "1", 1),
        ("1", "0", 2),
    ):
        build = _run(["-c", BUILD], {"APEX_CHUNK_INDEX": create_env})
        info = json.loads(build.stdout.strip().splitlines()[-1])
        assert info["encoding"] == encoding, (create_env, info)
        _run(["-c", READS, info["tmp"]], {"APEX_CHUNK_INDEX": rewrite_env})
        path = os.path.join(info["tmp"], "t.apex")
        with open(path, "rb") as handle:
            head = handle.read(256 + 32)
        version = int.from_bytes(head[8:12], "little")
        assert head[256 + 29] == encoding, (
            f"rewriting under APEX_CHUNK_INDEX={rewrite_env} changed the layout"
        )
        assert version == (5 if encoding == 2 else 4), (
            f"rewriting under APEX_CHUNK_INDEX={rewrite_env} changed the version"
        )
