"""Engine-level tests for the bounded-memory storage paths.

Covers the streaming compaction / schema-change rewrites that must never load
the whole table into memory: delta compaction, footer-only ADD COLUMN, and
streaming DROP COLUMN.
"""

import tempfile

from apexbase import ApexClient


def _seed(client, rows=50_000):
    client.create_table("events", {"name": "string", "score": "int"})
    client.use_table("events")
    client.store(
        [
            {"name": f"row_{i}", "score": i % 1000}
            for i in range(rows)
        ]
    )
    client.flush()


def test_alter_add_column_compacts_delta_and_synthesizes_nulls():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(str(tmp))
        _seed(client)

        # Pending delta rows (transactional INSERT) must survive compaction.
        client.execute("BEGIN")
        client.execute("INSERT INTO events (name, score) VALUES ('delta_a', 5000)")
        client.execute("COMMIT")

        client.execute("ALTER TABLE events ADD COLUMN note STRING")
        table_path = f"{tmp}/events.apex"
        assert not __import__("pathlib").Path(f"{table_path}.delta").exists()

        # Old rows: new column NULL; delta row present; values intact.
        rows = client.execute(
            "SELECT name, score, note FROM events ORDER BY _id LIMIT 3"
        ).to_dict()
        assert rows[0] == {"name": "row_0", "score": 0, "note": None}
        assert rows[2]["name"] == "row_2"
        tail = client.execute(
            "SELECT name, score, note FROM events WHERE name = 'delta_a'"
        ).to_dict()
        assert tail == [{"name": "delta_a", "score": 5000, "note": None}]

        # New writes include the column; old rows stay NULL.
        client.store([{"name": "new_row", "score": 7, "note": "hello"}])
        rows = client.execute(
            "SELECT name, note FROM events WHERE name = 'new_row'"
        ).to_dict()
        assert rows == [{"name": "new_row", "note": "hello"}]
        client.close()


def test_drop_middle_column_preserves_other_columns():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(str(tmp))
        client.create_table(
            "users",
            {"name": "string", "age": "int", "city": "string", "active": "int"},
        )
        client.use_table("users")
        client.store(
            [
                {"name": "Alice", "age": 25, "city": "NYC", "active": 1},
                {"name": "Bob", "age": 30, "city": "LA", "active": 0},
            ]
        )
        client.flush()
        client.drop_column("city")

        rows = client.execute("SELECT name, age, active FROM users ORDER BY _id").to_dict()
        assert rows == [
            {"name": "Alice", "age": 25, "active": 1},
            {"name": "Bob", "age": 30, "active": 0},
        ]
        assert client.list_fields() == ["name", "age", "active"]
        client.close()


def test_large_single_store_and_compaction_keep_rows():
    with tempfile.TemporaryDirectory() as tmp:
        client = ApexClient(str(tmp))
        _seed(client, rows=70_000)

        # Large single batch appends more than one Row Group.
        client.store(
            [{"name": f"extra_{i}", "score": i} for i in range(2_000)]
        )
        client.flush()

        count = client.execute("SELECT COUNT(*) AS c FROM events").to_dict()
        assert count == [{"c": 72_000}]
        spot = client.execute(
            "SELECT name FROM events WHERE score = 1234"
        ).to_dict()
        assert spot == [{"name": "extra_1234"}]
        client.close()
