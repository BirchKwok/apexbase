"""Vector columns: one established dimension per table, and row reads return it.

A fixed-size vector column is laid out as a fixed stride, so every row of every
row group must carry the same dimension. Appending a different dimension used to
be accepted: the new row group was written with its own dimension while the
table-level stride stayed at the first one, and every following read of that
column returned NULL or garbage. Mismatches are now rejected with a clear error
and nothing is written.

The same layout made `retrieve()`/point reads drop the column entirely (both
are covered in the older build too), so the row-value converter now returns the
raw row bytes, matching the columnar API's bytes representation.
"""

import struct

import pytest

from apexbase import ApexClient


@pytest.fixture
def client(tmp_path):
    c = ApexClient(str(tmp_path / "db"), drop_if_exists=True, enable_cache=False)
    try:
        yield c
    finally:
        c.close()


def _f32_bytes(values):
    return struct.pack(f"<{len(values)}f", *values)


def test_appending_a_different_dimension_is_rejected(client):
    client.create_table("t", {"v": "int64", "emb": "float32_vector"})
    client.use_table("t")
    client.store({"v": [0, 1, 2, 3], "emb": [[float(i)] * 16 for i in range(4)]})
    client.flush()
    assert client.execute("SELECT COUNT(*) AS n FROM t").to_dict() == [{"n": 4}]

    with pytest.raises(Exception) as excinfo:
        client.store({"v": [4, 5], "emb": [[float(i)] * 12 for i in range(2)]})
        client.flush()
    message = str(excinfo.value)
    assert "16-dimension" in message and "12-dimension" in message, message

    # Nothing was written: the table still holds exactly the original rows.
    assert client.execute("SELECT COUNT(*) AS n FROM t").to_dict() == [{"n": 4}]
    assert [row["v"] for row in client.execute("SELECT v FROM t ORDER BY v").to_dict()] == [
        0,
        1,
        2,
        3,
    ]


def test_a_rejected_append_leaves_the_vectors_readable(client):
    client.create_table("t", {"v": "int64", "emb": "float32_vector"})
    client.use_table("t")
    client.store({"v": [0, 1], "emb": [[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]]})
    client.flush()
    with pytest.raises(Exception):
        client.store({"v": [2], "emb": [[9.0, 10.0]]})
        client.flush()

    # The surviving column still decodes through the Arrow (scan) path.
    rows = client.execute("SELECT v, emb FROM t ORDER BY v").to_dict()
    assert [row["v"] for row in rows] == [0, 1]
    assert rows[0]["emb"] == pytest.approx([1.0, 2.0, 3.0, 4.0])
    assert rows[1]["emb"] == pytest.approx([5.0, 6.0, 7.0, 8.0])


def test_same_dimension_appends_keep_working(client):
    client.create_table("t", {"emb": "float32_vector"})
    client.use_table("t")
    client.store({"emb": [[1.0, 2.0, 3.0]]})
    client.flush()
    client.store({"emb": [[4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]})
    client.flush()
    client.store({"emb": [[10.0, 11.0, 12.0]]})
    client.flush()

    rows = client.execute("SELECT emb FROM t").to_dict()
    assert len(rows) == 4
    assert rows[0]["emb"] == pytest.approx([1.0, 2.0, 3.0])
    assert rows[3]["emb"] == pytest.approx([10.0, 11.0, 12.0])


def test_float16_columns_are_validated_too(client):
    client.create_table("t", {"emb": "float16_vector"})
    client.use_table("t")
    client.store({"emb": [[1.0] * 8, [2.0] * 8]})
    client.flush()

    with pytest.raises(Exception) as excinfo:
        client.store({"emb": [[3.0] * 4]})
        client.flush()
    assert "8-dimension" in str(excinfo.value), str(excinfo.value)
    assert client.execute("SELECT COUNT(*) AS n FROM t").to_dict() == [{"n": 2}]


def test_row_reads_return_the_vector_bytes(client):
    # `retrieve`/`WHERE _id = N` used to drop the column (NULL) even for a
    # healthy table; they now return the raw row bytes.
    client.create_table("t", {"v": "int64", "emb": "float32_vector"})
    client.use_table("t")
    client.store({"v": [0, 1], "emb": [[1.0, 2.0], [3.0, 4.0]]})
    client.flush()

    row = client.retrieve(2)
    assert row["v"] == 1
    assert bytes(row["emb"]) == _f32_bytes([3.0, 4.0])

    # The point-read lane returns the raw row bytes (same representation as
    # `retrieve`), while scan lanes decode to lists.
    point = client.execute("SELECT v, emb FROM t WHERE _id = 1").to_dict()[0]
    assert point["v"] == 0
    assert bytes(point["emb"]) == _f32_bytes([1.0, 2.0])

    scanned = client.execute("SELECT emb FROM t").to_dict()
    assert [row["emb"] for row in scanned] == [
        pytest.approx([1.0, 2.0]),
        pytest.approx([3.0, 4.0]),
    ]
    projected = client.execute("SELECT emb FROM t LIMIT 1").to_dict()
    assert projected[0]["emb"] == pytest.approx([1.0, 2.0])


def test_row_reads_work_before_and_after_a_flush(client):
    client.create_table("t", {"emb": "float32_vector"})
    client.use_table("t")
    client.store({"emb": [[1.0, 2.0, 3.0]]})
    # In-memory (unflushed) row.
    assert bytes(client.retrieve(1)["emb"]) == _f32_bytes([1.0, 2.0, 3.0])
    client.flush()
    # Persisted row goes through the mmap point-read path.
    assert bytes(client.retrieve(1)["emb"]) == _f32_bytes([1.0, 2.0, 3.0])


@pytest.mark.parametrize("compression", ["zstd", "lz4"])
def test_compressed_tables_point_read_vector_bytes(tmp_path, compression):
    """Vector point reads stay exact on compressed tables.

    A compressed row group has no RCIX, so the point read goes through the
    decompressed fallback; both paths now read the one row's bytes instead of
    materializing the whole vector column.
    """
    import struct

    def build(name, compression):
        client = ApexClient(str(tmp_path / name), drop_if_exists=True, enable_cache=False)
        client.create_table("t", {"v": "int64", "emb": "float32_vector"})
        client.use_table("t")
        if compression:
            assert client.set_compression(compression) is not False, compression
        client.store({"v": list(range(50)), "emb": [[float(i + j) for j in range(8)] for i in range(50)]})
        client.flush()
        return client

    plain = build("vec_plain", None)
    compressed = build(f"vec_{compression}", compression)
    try:
        for row_id in (1, 2, 25, 50):
            a = plain.retrieve(row_id)
            b = compressed.retrieve(row_id)
            assert a is not None and b is not None, row_id
            assert a["v"] == b["v"] == row_id - 1
            assert bytes(a["emb"]) == bytes(b["emb"]), row_id
            values = struct.unpack("<8f", bytes(b["emb"]))
            assert values == tuple(float(row_id - 1 + j) for j in range(8)), (row_id, values)
    finally:
        plain.close()
        compressed.close()
