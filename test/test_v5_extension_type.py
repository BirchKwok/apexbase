"""V5 S4: extension column type (D2 codec registry).

Covers declaring ``extension<codec>`` columns, encoding/decoding through the
Python codec registry (built-in ``bytes``/``json``), every read lane, reopen /
compression / cross-process behaviour, Arrow binary+metadata projection, and the
explicit error paths (unregistered codec, corrupt payload, version mismatch).
"""

import json

import pytest

from apexbase import ApexClient
from apexbase.extensions import (
    ExtensionCodecError,
    list_extension_codecs,
    register_extension_codec,
    unregister_extension_codec,
)


def _client(tmp_path, name="ext_t"):
    return ApexClient(str(tmp_path / name), drop_if_exists=True, enable_cache=False)


def test_extension_json_roundtrip_all_lanes(tmp_path):
    client = _client(tmp_path)
    try:
        client.create_table(
            "t",
            {
                "id": "int64",
                "payload": "extension<json>",
                "raw": "extension<bytes>",
                "label": "string",
            },
        )
        client.store(
            {
                "id": [1, 2, 3],
                "payload": [{"a": 1, "b": [1, 2]}, [1, 2, 3], "text"],
                "raw": [b"abc", b"\x00\x01", b""],
                "label": ["x", "y", "z"],
            }
        )
        client.flush()
        rows = client.execute("SELECT * FROM t").to_dict()
        assert [row["payload"] for row in rows] == [
            {"a": 1, "b": [1, 2]},
            [1, 2, 3],
            "text",
        ]
        assert [row["raw"] for row in rows] == [b"abc", b"\x00\x01", b""]

        projected = client.execute(
            "SELECT payload FROM t WHERE id >= 2"
        ).to_dict()
        assert [row["payload"] for row in projected] == [[1, 2, 3], "text"]
        aliased = client.execute("SELECT payload AS p FROM t LIMIT 1").to_dict()
        assert aliased[0]["p"] == {"a": 1, "b": [1, 2]}

        # Point lanes decode through the registry too.
        assert client.retrieve(1)["payload"] == {"a": 1, "b": [1, 2]}
        many = client.retrieve_many([1, 3]).to_dict()
        assert [row["payload"] for row in many] == [{"a": 1, "b": [1, 2]}, "text"]
        point_sql = client.execute("SELECT * FROM t WHERE _id = 2").to_dict()
        assert point_sql[0]["payload"] == [1, 2, 3]

        # String filter / LIKE lanes decode through the registry as well.
        client.store(
            {"id": [4], "payload": [{"tag": "needle"}], "raw": [b"z"], "label": ["needle"]}
        )
        client.flush()
        filtered = client.execute(
            "SELECT payload FROM t WHERE label = 'needle'"
        ).to_dict()
        assert filtered[0]["payload"] == {"tag": "needle"}

        # Arrow keeps the encoded bytes and adds extension metadata.
        import pyarrow as pa

        table = client.execute("SELECT * FROM t").to_arrow()
        assert pa.types.is_binary(table.schema.field("payload").type)
        metadata = table.schema.field("payload").metadata
        assert metadata[b"ARROW:extension:name"] == b"apexbase.json"
        assert metadata[b"apexbase.extension.codec"] == b"json"
        assert metadata[b"apexbase.extension.version"] == b"1"
        assert table.column("payload").to_pylist()[0] == json.dumps(
            {"a": 1, "b": [1, 2]}, ensure_ascii=False, separators=(",", ":")
        ).encode()
    finally:
        client.close()


def test_extension_reopen_compression_and_single_row_writes(tmp_path):
    client = _client(tmp_path, "ext_reopen")
    try:
        client.create_table("t", {"id": "int64", "payload": "extension<json>"})
        client.set_compression("zstd")
        client.store({"id": 1, "payload": {"kind": "one"}})
        client.store(
            [
                {"id": 2, "payload": {"kind": "two"}},
                {"id": 3, "payload": {"kind": "three"}},
            ]
        )
        client.flush()
    finally:
        client.close()

    reopened = ApexClient(str(tmp_path / "ext_reopen"), enable_cache=False)
    try:
        reopened.use_table("t")
        rows = reopened.execute("SELECT * FROM t").to_dict()
        assert [row["payload"] for row in rows] == [
            {"kind": "one"},
            {"kind": "two"},
            {"kind": "three"},
        ]
    finally:
        reopened.close()


def test_unregistered_codec_is_an_explicit_error(tmp_path):
    client = _client(tmp_path, "ext_unregistered")
    try:
        with pytest.raises(ExtensionCodecError):
            client.create_table("bad", {"payload": "extension<not_registered>"})
        with pytest.raises(ExtensionCodecError):
            register_extension_codec("bad codec", lambda v: b"", lambda b: b"")
    finally:
        client.close()


def test_pickle_requires_explicit_registration(tmp_path):
    client = _client(tmp_path, "ext_pickle")
    try:
        assert "pickle" not in list_extension_codecs()
        with pytest.raises(ExtensionCodecError):
            client.create_table("t", {"payload": "extension<pickle>"})

        import pickle

        register_extension_codec("pickle", pickle.dumps, pickle.loads)
        try:
            client.create_table("t", {"id": "int64", "payload": "extension<pickle>"})
            client.store(
                {"id": [1], "payload": [{"nested": [1, {"x": (2, 3)}]}]}
            )
            client.flush()
            rows = client.execute("SELECT * FROM t").to_dict()
            assert rows[0]["payload"] == {"nested": [1, {"x": (2, 3)}]}
        finally:
            unregister_extension_codec("pickle")
    finally:
        client.close()


def test_codec_version_mismatch_is_an_explicit_error(tmp_path):
    client = _client(tmp_path, "ext_version")
    try:
        client.create_table("t", {"id": "int64", "payload": "extension<json>"})
        client.store({"id": [1], "payload": [{"v": 1}]})
        client.flush()
        # Re-register the same codec under a new version: stored columns are v1
        # and must fail loudly on both read and write.
        register_extension_codec(
            "json",
            lambda value: json.dumps(value).encode(),
            lambda payload: json.loads(payload),
            version=2,
        )
        try:
            with pytest.raises(ExtensionCodecError):
                client.execute("SELECT * FROM t").to_dict()
            with pytest.raises(ExtensionCodecError):
                client.create_table("v2", {"payload": "extension<json@1>"})
        finally:
            register_extension_codec(
                "json",
                lambda value: json.dumps(
                    value, ensure_ascii=False, separators=(",", ":")
                ).encode(),
                lambda payload: json.loads(payload),
                version=1,
            )
        # Restoring the matching version makes the table readable again.
        assert client.execute("SELECT * FROM t").to_dict()[0]["payload"] == {"v": 1}
    finally:
        client.close()


def test_corrupt_payload_is_an_explicit_error(tmp_path):
    def encode(value):
        return b"fragile:" + json.dumps(value).encode()

    def decode(payload):
        if not payload.startswith(b"fragile:"):
            raise ValueError("bad frame")
        return json.loads(payload[len(b"fragile:"):])

    register_extension_codec("fragile", encode, decode)
    client = _client(tmp_path, "ext_corrupt")
    try:
        client.create_table("t", {"payload": "extension<fragile>"})
        client.store({"payload": [{"ok": True}]})
        client.flush()
        assert client.execute("SELECT * FROM t").to_dict()[0]["payload"] == {"ok": True}
        # A codec that rejects the stored payload must raise, never fall back to
        # raw bytes/strings.
        register_extension_codec(
            "fragile",
            encode,
            lambda payload: (_ for _ in ()).throw(ValueError("corrupt payload")),
        )
        try:
            with pytest.raises(ExtensionCodecError):
                client.execute("SELECT * FROM t").to_dict()
        finally:
            register_extension_codec("fragile", encode, decode)
        # Unregistering the codec makes the column unreadable explicitly.
        unregister_extension_codec("fragile")
        try:
            with pytest.raises(ExtensionCodecError):
                client.execute("SELECT * FROM t").to_dict()
        finally:
            register_extension_codec("fragile", encode, decode)
    finally:
        client.close()
        unregister_extension_codec("fragile")


def test_extension_query_shapes_and_table_types(tmp_path):
    client = _client(tmp_path, "ext_query")
    try:
        client.create_table("t", {"id": "int64", "payload": "extension<json>"})
        client.store(
            {"id": [1, 2], "payload": [{"a": 1}, {"a": 2}]}
        )
        client.flush()
        declared = dict(client._storage.table_types("t"))
        assert declared["payload"] == "extension<json>"
        # NULL rows keep the D1 NULL contract.
        client.store({"id": [3], "payload": [None]})
        client.flush()
        nulls = client.execute("SELECT payload FROM t WHERE id = 3").to_dict()
        assert nulls[0]["payload"] is None
        for sql in (
            "SELECT MIN(payload) FROM t",
            "SELECT SUM(payload) FROM t",
            "SELECT payload FROM t ORDER BY payload",
        ):
            with pytest.raises(Exception):
                client.execute(sql).to_dict()
    finally:
        client.close()
