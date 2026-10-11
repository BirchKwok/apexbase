"""
Test cases for column encoding compatibility.
Tests various string encodings that may be encountered from different systems.
"""
import apexbase as ab
from apexbase import ApexClient
import tempfile
import pytest


class TestColumnEncoding:
    """Test various column encoding scenarios"""

    def test_create_index_on_string_column(self):
        """Test creating index on regular string column"""
        with tempfile.TemporaryDirectory() as tmpdir:
            client = ab.ApexClient(dirpath=tmpdir)
            client.create_table('test')

            # Insert test data
            data = [{'col': f'value_{i}'} for i in range(100)]
            client.store(data)

            # Create index
            client.execute('CREATE INDEX idx_col ON test (col)')
            print('Index created successfully')

            # Query using index
            result = client.execute("SELECT * FROM test WHERE col = 'value_50'")
            assert len(result) == 1
            assert result[0]['col'] == 'value_50'

            client.close()

    def test_create_index_on_dict_encoded_column(self):
        """Test creating index on dictionary-encoded column (low cardinality)"""
        with tempfile.TemporaryDirectory() as tmpdir:
            client = ab.ApexClient(dirpath=tmpdir)
            client.create_table('test')

            # Insert test data with low cardinality (good for dict encoding)
            data = [{'category': f'cat_{i % 5}'} for i in range(1000)]
            client.store(data)

            # Create index on low-cardinality column
            client.execute('CREATE INDEX idx_category ON test (category)')
            print('Index on dict-encoded column created successfully')

            # Query using index
            result = client.execute("SELECT * FROM test WHERE category = 'cat_2'")
            assert len(result) == 200

            client.close()

    def test_unicode_string_column(self):
        """Test Unicode string handling"""
        with tempfile.TemporaryDirectory() as tmpdir:
            client = ab.ApexClient(dirpath=tmpdir)
            client.create_table('test')

            # Insert Unicode data
            data = [
                {'text': '你好世界'},
                {'text': 'Hello World'},
                {'text': '日本語テスト'},
                {'text': '🔒 Emoji Test'},
            ]
            client.store(data)

            # Create index
            client.execute('CREATE INDEX idx_text ON test (text)')
            print('Index on Unicode column created successfully')

            # Query Unicode
            result = client.execute("SELECT * FROM test WHERE text = '你好世界'")
            assert len(result) == 1

            client.close()

    def test_special_characters_column(self):
        """Test special characters in strings"""
        with tempfile.TemporaryDirectory() as tmpdir:
            client = ab.ApexClient(dirpath=tmpdir)
            client.create_table('test')

            # Insert data with special characters
            data = [
                {'text': 'line1\nline2'},
                {'text': 'tab\there'},
                {'text': 'quote"here'},
                {'text': 'backslash\\here'},
            ]
            client.store(data)

            # Create index
            client.execute('CREATE INDEX idx_text ON test (text)')
            print('Index on special chars column created successfully')

            client.close()

    def test_large_string_column(self):
        """Test large string values"""
        with tempfile.TemporaryDirectory() as tmpdir:
            client = ab.ApexClient(dirpath=tmpdir)
            client.create_table('test')

            # Insert large string data
            large_text = 'x' * 10000
            data = [
                {'text': large_text},
                {'text': 'short'},
                {'text': 'medium' * 100},
            ]
            client.store(data)

            # Create index
            client.execute('CREATE INDEX idx_text ON test (text)')
            print('Index on large string column created successfully')

            client.close()

    def test_multiple_string_columns(self):
        """Test multiple string columns with different characteristics"""
        with tempfile.TemporaryDirectory() as tmpdir:
            client = ab.ApexClient(dirpath=tmpdir)
            client.create_table('test')

            # Insert data with multiple string columns
            data = []
            for i in range(500):
                data.append({
                    'low_card': f'type_{i % 3}',  # Very low cardinality
                    'med_card': f'group_{i % 50}',  # Medium cardinality
                    'high_card': f'unique_{i}',  # High cardinality
                })
            client.store(data)

            # Create indexes on all string columns
            client.execute('CREATE INDEX idx_low ON test (low_card)')
            client.execute('CREATE INDEX idx_med ON test (med_card)')
            client.execute('CREATE INDEX idx_high ON test (high_card)')
            print('Indexes on multiple string columns created successfully')

            # Query using different indexes
            result1 = client.execute("SELECT * FROM test WHERE low_card = 'type_1'")
            assert len(result1) > 0

            result2 = client.execute("SELECT * FROM test WHERE med_card = 'group_25'")
            assert len(result2) > 0

            client.close()

    def test_mixed_type_columns_with_string(self):
        """Test table with mixed types including string"""
        with tempfile.TemporaryDirectory() as tmpdir:
            client = ab.ApexClient(dirpath=tmpdir)
            client.create_table('test')

            # Insert mixed type data
            data = []
            for i in range(200):
                data.append({
                    'id': i,
                    'name': f'name_{i}',
                    'value': float(i) * 1.5,
                    'active': i % 2 == 0,
                })
            client.store(data)

            # Create index on string column
            client.execute('CREATE INDEX idx_name ON test (name)')
            print('Index on mixed-type table string column created successfully')

            # Query
            result = client.execute("SELECT * FROM test WHERE name = 'name_100'")
            assert len(result) == 1
            assert result[0]['id'] == 100

            client.close()


class TestDictionaryDropOnCompaction:
    """A compaction may drop dictionary encoding; the footer must follow.

    `dict_encode_if_smaller` declines below 1000 rows, which a delete-then-compact
    can cross. The writer used to keep the *old* schema type in that case, so the
    footer claimed a dictionary while the body held plain strings. Readers that
    take the column type from the schema (the length-prefixed row groups do) then
    parsed plain bytes as a dictionary: on a zstd table `SELECT s` silently
    returned `''` and `GROUP BY s` failed with "StringDict dict_offsets truncated",
    while numbers and uncompressed tables looked fine.
    """

    @pytest.mark.parametrize("compression", [None, "zstd"])
    @pytest.mark.parametrize("total,keep", [(2000, 999), (2000, 500), (5000, 1), (5000, 1000)])
    def test_values_survive_a_compaction_that_drops_the_dictionary(
        self, compression, total, keep
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            # A low-cardinality column over >= 1000 rows is dictionary encoded.
            client = ab.ApexClient(dirpath=tmpdir, enable_cache=False)
            client.create_table("t", {"c0": "int64", "s": "string"})
            client.use_table("t")
            if compression:
                assert client.set_compression(compression) is not False
            client.store(
                {
                    "c0": list(range(total)),
                    "s": [f"v{index % 97}" for index in range(total)],
                }
            )
            client.flush()
            client.execute(f"DELETE FROM t WHERE c0 < {total - keep}")
            client.flush()
            client.close()

            # Read through a fresh client so the footer is parsed from disk.
            client = ab.ApexClient(dirpath=tmpdir, enable_cache=False)
            client.use_table("t")
            want = [f"v{index % 97}" for index in range(total - keep, total)]
            assert [row["s"] for row in client.execute("SELECT s FROM t").to_dict()] == want
            assert [
                row["s"] for row in client.execute("SELECT s FROM t LIMIT 5").to_dict()
            ] == want[:5]
            assert len(
                client.execute("SELECT s, count(*) AS n FROM t GROUP BY s").to_dict()
            ) == len(set(want))
            assert client.execute("SELECT count(s) AS n FROM t").to_dict()[0]["n"] == keep
            # The last row is still readable by id, with its own value.
            assert client.execute(
                f"SELECT s FROM t WHERE _id = {total}"
            ).to_dict() == [{"s": f"v{(total - 1) % 97}"}]
            client.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
