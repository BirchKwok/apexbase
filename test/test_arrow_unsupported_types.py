"""W4-4: importing an Arrow column of an unsupported type must fail loudly.

The schema mapper used to call every other Arrow type "string", silently storing
decimals/lists/structs as text. Unsupported types now raise; dictionary<string>
and fixed-size binary keep working.
"""
from __future__ import annotations

import pytest

from apexbase.client import ApexClient

pa = pytest.importorskip("pyarrow")


@pytest.mark.parametrize(
    "array",
    [
        pa.array([1, 2], type=pa.decimal128(10, 2)),
        pa.array([[1, 2]], type=pa.list_(pa.int64())),
        pa.array([(1, "a")], type=pa.struct([("x", pa.int64()), ("y", pa.string())])),
        pa.array([1, 2], type=pa.time32("s")),
    ],
)
def test_unsupported_arrow_types_raise(array):
    schema = pa.schema([pa.field("value", array.type)])
    with pytest.raises(ValueError, match="unsupported type"):
        ApexClient._arrow_schema_to_apex_schema(schema)


def test_supported_arrow_types_keep_their_storage_type():
    schema = pa.schema(
        [
            pa.field("d", pa.date32()),
            pa.field("ts", pa.timestamp("us")),
            pa.field("fixed", pa.binary(4)),
            pa.field("dict_str", pa.dictionary(pa.int32(), pa.string())),
            pa.field("vec", pa.list_(pa.float32(), 4)),
        ]
    )
    mapped = ApexClient._arrow_schema_to_apex_schema(schema)
    assert mapped == {
        "d": "date",
        "ts": "timestamp",
        "fixed": "binary",
        "dict_str": "string",
        "vec": "float16_vector",
    }
