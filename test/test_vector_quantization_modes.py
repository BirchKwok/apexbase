import numpy as np
import pytest

from apexbase.client import ApexClient


VECTOR_TYPES = [
    "FLOAT32_VECTOR",
    "FLOAT16_VECTOR",
    "BFLOAT16_VECTOR",
    "INT8_VECTOR",
    "UINT8_VECTOR",
    "BIT1_VECTOR",
    "TURBOQUANT2_VECTOR",
    "TURBOQUANT3_VECTOR",
    "TURBOQUANT4_VECTOR",
]


@pytest.mark.parametrize("vector_type", VECTOR_TYPES)
def test_quantized_vector_roundtrip_topk_batch_and_reopen(tmp_path, vector_type):
    db = str(tmp_path / vector_type.lower())
    rng = np.random.default_rng(20260821)
    vectors = rng.normal(size=(96, 13)).astype(np.float32)
    query_index = 17

    client = ApexClient(dirpath=db, drop_if_exists=True)
    client.execute(f"CREATE TABLE vectors (name TEXT, vec {vector_type})")
    client.use_table("vectors")
    client.store([{"name": str(i), "vec": row} for i, row in enumerate(vectors)])

    count = client.execute("SELECT COUNT(*) AS n FROM vectors").to_dict()[0]["n"]
    assert count == len(vectors)
    dims = client.execute("SELECT vector_dim(vec) AS d FROM vectors LIMIT 3").to_dict()
    assert [row["d"] for row in dims] == [13, 13, 13]

    single = client.topk_distance("vec", vectors[query_index], k=5, metric="l2").to_dict()
    assert query_index + 1 in {int(row["_id"]) for row in single}
    assert [row["dist"] for row in single] == sorted(row["dist"] for row in single)

    batch = client.batch_topk_distance(
        "vec", vectors[[query_index, 31]], k=5, metric="cosine_distance"
    )
    assert batch.shape == (2, 5, 2)
    assert np.all(batch[:, 1:, 1] >= batch[:, :-1, 1])
    client.close()

    reopened = ApexClient(dirpath=db, drop_if_exists=False)
    reopened.use_table("vectors")
    after_reopen = reopened.topk_distance(
        "vec", vectors[query_index], k=5, metric="l2"
    ).to_dict()
    assert [row["_id"] for row in after_reopen] == [row["_id"] for row in single]
    reopened.close()


@pytest.mark.parametrize(
    "alias",
    ["F32_VECTOR", "F16_VECTOR", "BF16_VECTOR", "I8_VECTOR", "U8_VECTOR",
     "BINARY1_VECTOR", "TQ2_VECTOR", "TQ3_VECTOR", "TQ4_VECTOR"],
)
def test_quantized_vector_sql_aliases(tmp_path, alias):
    client = ApexClient(dirpath=str(tmp_path / alias.lower()), drop_if_exists=True)
    client.execute(f"CREATE TABLE vectors (vec {alias})")
    client.close()


@pytest.mark.parametrize("vector_type", VECTOR_TYPES[2:])
def test_quantized_vector_rejects_ragged_and_non_finite_input(tmp_path, vector_type):
    client = ApexClient(dirpath=str(tmp_path / vector_type.lower()), drop_if_exists=True)
    client.execute(f"CREATE TABLE vectors (vec {vector_type})")
    client.use_table("vectors")
    with pytest.raises(ValueError, match="dimension"):
        client.store([{"vec": [1.0, 2.0]}, {"vec": [1.0, 2.0, 3.0]}])
    with pytest.raises(ValueError, match="finite"):
        client.store([{"vec": [1.0, np.nan]}, {"vec": [2.0, 3.0]}])
    client.close()


def test_turboquant_recall_improves_or_holds_with_bit_width(tmp_path):
    rng = np.random.default_rng(42)
    vectors = rng.normal(size=(512, 64)).astype(np.float32)
    queries = vectors[:24] + rng.normal(scale=0.01, size=(24, 64)).astype(np.float32)
    exact = np.argsort(((queries[:, None, :] - vectors[None, :, :]) ** 2).sum(axis=2), axis=1)[:, :10]
    recalls = []
    for bits in (2, 3, 4):
        client = ApexClient(dirpath=str(tmp_path / f"tq{bits}"), drop_if_exists=True)
        client.execute(f"CREATE TABLE vectors (vec TURBOQUANT{bits}_VECTOR)")
        client.use_table("vectors")
        client.store([{"vec": row} for row in vectors])
        approximate = client.batch_topk_distance("vec", queries, k=10, metric="l2")[:, :, 0]
        recall = np.mean([
            len(set((approximate[i] - 1).astype(int)) & set(exact[i])) / 10
            for i in range(len(queries))
        ])
        recalls.append(recall)
        client.close()
    assert recalls[0] >= 0.30
    assert recalls[1] + 0.03 >= recalls[0]
    assert recalls[2] + 0.03 >= recalls[1]


def test_stored_quantized_column_rescores_and_can_be_dropped(tmp_path):
    rng = np.random.default_rng(20260821)
    vectors = rng.normal(size=(128, 24)).astype(np.float32)
    query = vectors[37] + rng.normal(scale=0.03, size=24).astype(np.float32)

    db = str(tmp_path / "derived")
    client = ApexClient(dirpath=db, drop_if_exists=True)
    client.create_table("items", {"label": "int64", "embedding": "float32_vector"})
    client.store(
        [
            {"label": index, "embedding": vector}
            for index, vector in enumerate(vectors[:96])
        ]
    )
    target = client.create_quantized_column(
        source="embedding",
        target="embedding_tq4",
        codec="turboquant4",
    )
    assert target == "embedding_tq4"
    client.store(
        [
            {"label": index, "embedding": vector}
            for index, vector in enumerate(vectors[96:], start=96)
        ]
    )
    client.close()

    reopened = ApexClient(dirpath=db, drop_if_exists=False)
    reopened.use_table("items")
    assert reopened.replace(1, {"label": 0, "embedding": query})
    reopened.close()

    reopened = ApexClient(dirpath=db, drop_if_exists=False)
    reopened.use_table("items")
    projected = reopened.execute(
        "SELECT vector_dim(embedding_tq4) AS d FROM items LIMIT 3"
    ).to_dict()
    assert [row["d"] for row in projected] == [24, 24, 24]
    exact = reopened.topk_distance("embedding", query, k=10, metric="l2").to_dict()
    rescored = reopened.topk_distance(
        "embedding",
        query,
        k=10,
        metric="l2",
        accelerator="embedding_tq4",
        candidate_k=64,
    ).to_dict()
    assert rescored == exact

    with pytest.raises(Exception, match="depend"):
        reopened._storage.drop_column("embedding")
    with pytest.raises(Exception, match="not a registered quantized accelerator"):
        reopened.drop_quantized_column("label")
    reopened.drop_quantized_column("embedding_tq4")
    assert reopened.topk_distance("embedding", query, k=10, metric="l2").to_dict() == exact
    with pytest.raises(Exception, match="not a registered quantized accelerator|not found"):
        reopened.topk_distance(
            "embedding",
            query,
            k=10,
            accelerator="embedding_tq4",
            candidate_k=64,
        )


@pytest.mark.parametrize(
    "source_type",
    ["float32_vector", "float16_vector", "bfloat16_vector"],
)
def test_quantized_accelerator_tracks_supported_source_precisions(tmp_path, source_type):
    rng = np.random.default_rng(73)
    vectors = rng.normal(size=(48, 11)).astype(np.float32)
    client = ApexClient(dirpath=str(tmp_path / source_type), drop_if_exists=True)
    client.create_table("vectors", {"vec": source_type})
    client.store([{"vec": vector} for vector in vectors[:32]])
    target = client.create_quantized_column("vec", codec="int8")
    client.store([{"vec": vector} for vector in vectors[32:]])

    exact = client.topk_distance("vec", vectors[9], k=6).to_dict()
    rescored = client.topk_distance(
        "vec",
        vectors[9],
        k=6,
        accelerator=target,
        candidate_k=48,
    ).to_dict()
    assert rescored == exact
    client.drop_quantized_column(target)
    assert client.topk_distance("vec", vectors[9], k=6).to_dict() == exact
    client.close()


def test_quantized_rescore_falls_back_for_compressed_row_groups(tmp_path):
    rng = np.random.default_rng(91)
    vectors = rng.normal(size=(64, 15)).astype(np.float32)
    db = str(tmp_path / "compressed")
    client = ApexClient(dirpath=db, drop_if_exists=True)
    client.create_table("vectors", {"vec": "float32_vector"})
    client.set_compression("zstd")
    client.store([{"vec": vector} for vector in vectors])
    target = client.create_quantized_column("vec", codec="turboquant3")
    client.close()

    client = ApexClient(dirpath=db, drop_if_exists=False)
    client.use_table("vectors")
    exact = client.topk_distance("vec", vectors[22], k=8).to_dict()
    rescored = client.topk_distance(
        "vec",
        vectors[22],
        k=8,
        accelerator=target,
        candidate_k=64,
    ).to_dict()
    assert rescored == exact
    client.close()
