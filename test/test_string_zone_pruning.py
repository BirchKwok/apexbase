"""Zone-map pruning for string columns.

A string column carries two zone entries: the byte-length range and (V5) the
order-preserving prefix of the lexicographic value range. A scan skips a row
group when *either* range proves the predicate cannot match, so the legacy
length pruning survives (decisive when the value length tracks the row range,
e.g. `user_{i}`) and the value range adds pruning where all values share a
length (e.g. fixed-width codes). Equality, `IN`, `BETWEEN` and the ordering
comparisons all benefit from the value range.

The prefix covers the first eight bytes. Values that share those eight bytes can
never be told apart by the value range, which is the conservative case (no
pruning, never a wrong skip) and is covered here too.
"""

import os
import shutil
import struct
import tempfile

import pytest

from apexbase import ApexClient

ROWS = 400_000
BUCKET = 131_072  # largest adaptive row-group size: groups stay inside a bucket


def _row_group_count(path):
    with open(path, "rb") as handle:
        header = handle.read(88)
    return struct.unpack_from("<I", header, 84)[0]


def _string_zone_entries(path, column):
    """Per row group, the (min, max, kind) entries of `column`.

    Every string column carries three entries: the byte-length range first (so
    readers that predate the value-range flags keep the legacy meaning), then
    the lexicographic prefix-key range (first eight bytes), then the tail range
    (second eight bytes). A scan prunes a row group when any range proves the
    predicate cannot match.
    """
    with open(path, "rb") as handle:
        blob = handle.read()
    footer_offset = struct.unpack_from("<Q", blob, 76)[0]
    footer = blob[footer_offset:]
    schema_len = struct.unpack_from("<Q", footer, 0)[0]
    schema = footer[8 : 8 + schema_len]
    column_count = struct.unpack_from("<I", schema, 0)[0]
    pos = 4
    names = []
    for _ in range(column_count):
        name_len = struct.unpack_from("<H", schema, pos)[0]
        pos += 2
        names.append(schema[pos : pos + name_len].decode())
        pos += name_len + 1  # name bytes + type byte
    target = names.index(column)

    zmap = footer_offset + footer.find(b"ZMAP")
    group_count = struct.unpack_from("<I", blob, zmap + 4)[0]
    pos = zmap + 8
    entries = []
    for _ in range(group_count):
        per_group = []
        entry_count = struct.unpack_from("<H", blob, pos)[0]
        pos += 2
        for _ in range(entry_count):
            col_idx = struct.unpack_from("<H", blob, pos)[0]
            minimum, maximum = struct.unpack_from("<qq", blob, pos + 2)
            flags = blob[pos + 18]
            if col_idx == target:
                if flags & 4:
                    kind = "prefix"
                elif flags & 8:
                    kind = "tail"
                else:
                    kind = "length"
                per_group.append((minimum, maximum, kind))
            pos += 19
        entries.append(per_group)
    return entries


def _window_key(value, offset):
    """Order-preserving i64 key of the eight bytes at `offset` (writer parity)."""
    window = (value[offset : offset + 8] + b"\x00" * 8)[:8]
    raw = int.from_bytes(window, "big") ^ (1 << 63)
    return raw - (1 << 64)  # always negative: the sign bit is flipped on


# Values: one constant value per bucket, so a bucket is a contiguous run of
# identical strings (the shape zone maps are meant to prune).
BUCKET_VALUES = [f"b{i // BUCKET:07d}" for i in range(ROWS)]
PARTIAL = ROWS - 3 * BUCKET  # rows in the last, incomplete bucket


def _columns(rows):
    return {
        # The distinguishing part is inside the first eight bytes.
        "name": list(BUCKET_VALUES),
        # Same eight-byte prefix for every row: never prunable.
        "shared": [f"b0000000-shared-{i}" for i in range(rows)],
        "pad": list(range(rows)),
    }


@pytest.fixture
def client():
    root = tempfile.mkdtemp()
    c = ApexClient(root, enable_cache=False)
    c.create_table("t", schema={"name": "string", "shared": "string", "pad": "int64"})
    c.use_table("t")
    c.store(_columns(ROWS))
    c.flush()
    groups = _row_group_count(os.path.join(root, "t.apex"))
    assert groups >= 3, f"fixture needs multiple row groups, got {groups}"
    try:
        yield c
    finally:
        c.close()
        shutil.rmtree(root, ignore_errors=True)


def _count(c, where):
    return c.execute(f"SELECT COUNT(*) AS n FROM t WHERE {where}").to_dict()[0]["n"]


def test_equality_matches_only_its_bucket(client):
    assert _count(client, "name = 'b0000001'") == BUCKET
    assert _count(client, "name = 'b0000002'") == BUCKET
    # A value that exists nowhere prunes everything.
    assert _count(client, "name = 'nope'") == 0


def test_ranges_and_ordering_stay_exact(client):
    assert _count(client, "name < 'b0000001'") == BUCKET
    assert _count(client, "name <= 'b0000001'") == 2 * BUCKET
    assert _count(client, "name > 'b0000002'") == PARTIAL
    assert _count(client, "name >= 'b0000002'") == BUCKET + PARTIAL
    assert _count(client, "name BETWEEN 'b0000001' AND 'b0000002'") == 2 * BUCKET
    # A window strictly between two bucket values matches nothing.
    assert _count(client, "name > 'b0000001' AND name < 'b0000002'") == 0


def test_in_list_and_negation(client):
    assert _count(client, "name IN ('b0000001', 'b0000003', 'missing')") == BUCKET + PARTIAL
    assert _count(client, "name != 'b0000001'") == ROWS - BUCKET
    assert _count(client, "NOT (name = 'b0000001')") == ROWS - BUCKET


def test_values_sharing_the_prefix_are_never_skipped(client):
    # `shared` starts with the same eight bytes on every row, so the zone map
    # cannot separate them: the predicate must still return every match.
    assert _count(client, "shared = 'b0000000-shared-7'") == 1
    assert _count(client, "shared IN ('b0000000-shared-1', 'b0000000-shared-2')") == 2
    rows = client.execute(
        "SELECT shared FROM t WHERE shared = 'b0000000-shared-199999'"
    ).to_dict()
    assert rows == [{"shared": "b0000000-shared-199999"}]


def test_length_zone_proves_a_prefix_match_absent(client):
    # Every `shared` value starts with the same eight bytes, so the value range
    # keeps all groups; only the byte-length zone can prove this probe absent.
    probe = "b0000000-shared-7" + "x" * 40
    assert _count(client, f"shared = '{probe}'") == 0
    # A probe with a stored length still matches exactly the stored row.
    assert _count(client, "shared = 'b0000000-shared-7'") == 1


def test_writer_records_only_the_zones_that_carry_information():
    # The length zone must be written first: readers that predate the value-range
    # flags resolve a string column's zone with the first non-float entry. The
    # tail zone is written only when the value zone is degenerate (every value in
    # the group shares its first eight bytes) *and* the tails differ, because
    # otherwise it repeats what is already stored.
    root = tempfile.mkdtemp()
    try:
        rows = ROWS
        c = ApexClient(root, enable_cache=False)
        c.create_table("t", schema={"name": "string", "code": "string", "pad": "int64"})
        c.use_table("t")
        c.store(
            {
                "name": [f"user_{i}" for i in range(rows)],
                "code": [f"2026-09-26T{i:06d}" for i in range(rows)],
                "pad": list(range(rows)),
            }
        )
        c.flush()
        assert _count(c, "name = 'user_5000'") == 1
        assert _count(c, "code = '2026-09-26T000500'") == 1
        c.close()

        path = os.path.join(root, "t.apex")
        # `user_{i}` varies in its first eight bytes per group, so the value zone
        # already separates values: no tail zone is written.
        names = _string_zone_entries(path, "name")
        assert len(names) >= 3, "fixture needs multiple row groups"
        for per_group in names:
            assert [zone[2] for zone in per_group] == ["length", "prefix"]
            assert per_group[0][0] <= per_group[0][1]
            assert per_group[1][0] <= per_group[1][1]
        assert names[0][0][0] == len("user_0")
        assert names[1][0][0] > len("user_5000")

        # Every timestamp shares its first eight bytes inside a group, and the
        # tails differ, so the tail zone is written.
        codes = _string_zone_entries(path, "code")
        for per_group in codes:
            assert [zone[2] for zone in per_group] == ["length", "prefix", "tail"]
            length_zone, prefix_zone, tail_zone = per_group
            assert (length_zone[0], length_zone[1]) == (17, 17)
            assert prefix_zone[0] == prefix_zone[1], "shared first eight bytes"
            assert tail_zone[0] < tail_zone[1], "tails vary inside the group"
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_tail_zone_prunes_equal_length_shared_prefix_column():
    # Timestamp-shaped values are all the same length and share their first
    # eight bytes: the length zone and the prefix zone are constant across every
    # group, so only the tail zone can make the column prunable.
    root = tempfile.mkdtemp()
    try:
        rows = 400_000
        codes = [f"2026-09-26T{i:06d}" for i in range(rows)]
        assert len(codes[0]) == 17 and codes[0][:8] == codes[-1][:8]
        c = ApexClient(root, enable_cache=False)
        c.create_table("t", schema={"code": "string", "pad": "int64"})
        c.use_table("t")
        c.store({"code": codes, "pad": list(range(rows))})
        c.flush()
        first, middle, last = codes[0], codes[rows // 2], codes[-1]
        assert _count(c, f"code = '{first}'") == 1
        assert _count(c, f"code = '{middle}'") == 1
        assert _count(c, f"code = '{last}'") == 1
        assert _count(c, f"code IN ('{first}', '{last}', 'nope')") == 2
        # A probe that shares the first sixteen stored bytes matches nothing but
        # must stay answerable: the scan cannot prove it absent from the zones.
        assert _count(c, f"code = '{middle[:16]}x'") == 0
        assert _count(c, f"code = '{middle}'") == 1
        # Ordering stays exact.
        assert _count(c, f"code > '{first}'") == rows - 1
        assert _count(c, f"code BETWEEN '{first}' AND '{codes[1]}'") == 2
        c.close()

        entries = _string_zone_entries(os.path.join(root, "t.apex"), "code")
        assert len(entries) >= 3, "fixture needs multiple row groups"
        lengths = {zone[:2] for per_group in entries for zone in per_group if zone[2] == "length"}
        prefixes = {zone[:2] for per_group in entries for zone in per_group if zone[2] == "prefix"}
        tails = {zone[:2] for per_group in entries for zone in per_group if zone[2] == "tail"}
        # Constant width and constant first eight bytes: the two value-blind
        # zones are identical everywhere, so any pruning must come from the tail.
        assert lengths == {(17, 17)}
        assert len(prefixes) == 1
        assert len(tails) > 1
        # The group that holds the middle value is the only one whose tail range
        # can contain it, and a far-away group provably cannot.
        middle_tail = _window_key(middle.encode(), 8)
        containing = [per_group for per_group in entries if any(
            zone[2] == "tail" and zone[0] <= middle_tail <= zone[1] for zone in per_group
        )]
        assert len(containing) == 1, "exactly one group's tail range can hold it"
        last_tail = next(zone for zone in entries[-1] if zone[2] == "tail")
        first_tail = _window_key(first.encode(), 8)
        assert not (
            last_tail[0] <= first_tail <= last_tail[1]
        ), "the last group's tail range cannot hold the first value"
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_pruning_survives_appends_and_updates(client):
    # A second bucket appended later must be prunable too, and an in-place
    # update must keep the zone honest for the value it introduced.
    client.store(
        {
            "name": ["zzzz-extra"] * 10,
            "shared": ["b0000000-shared-extra"] * 10,
            # Distinct from every existing row so the UPDATE below targets one.
            "pad": [-1] * 10,
        }
    )
    client.flush()
    assert _count(client, "name = 'zzzz-extra'") == 10
    assert _count(client, "name = 'b0000001'") == BUCKET

    updated = client.execute("UPDATE t SET name = 'b0000001' WHERE pad = 0").to_dict()
    assert updated == [{"result": 1}]
    assert _count(client, "name = 'b0000001'") == BUCKET + 1
    # The scan/aggregate lane agrees with the point read.
    assert client.retrieve(1)["name"] == "b0000001"
