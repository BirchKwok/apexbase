//! Data type definitions

use serde::{Deserialize, Serialize};

/// Supported data types in ApexBase
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum DataType {
    Null = 0,
    Bool = 1,
    Int8 = 2,
    Int16 = 3,
    Int32 = 4,
    Int64 = 5,
    UInt8 = 6,
    UInt16 = 7,
    UInt32 = 8,
    UInt64 = 9,
    Float32 = 10,
    Float64 = 11,
    String = 12,
    Binary = 13,
    Json = 14,
    Timestamp = 15,
    Date = 16,
    Array = 17,
    Decimal = 18,
    Float16Vector = 19,
    Blob = 20,
    Float32Vector = 21,
    BFloat16Vector = 22,
    Int8Vector = 23,
    UInt8Vector = 24,
    Bit1Vector = 25,
    TurboQuant2Vector = 26,
    TurboQuant3Vector = 27,
    TurboQuant4Vector = 28,
    Uuid = 29,
    Decimal128 = 30,
    /// Time of day without a date or time zone (microseconds since midnight).
    Time = 31,
    /// Duration/interval (signed microseconds). Presented as `timedelta`.
    Interval = 32,
    /// Timestamp with a fixed UTC offset; the physical payload is the same
    /// 8-byte UTC microseconds as `Timestamp` and `LogicalType::TimestampTz`
    /// carries the display offset.
    TimestampTz = 33,
}

impl DataType {
    /// Get the fixed size of this data type in bytes, or None for variable-length types
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            DataType::Null => Some(0),
            DataType::Bool => Some(1),
            DataType::Int8 | DataType::UInt8 => Some(1),
            DataType::Int16 | DataType::UInt16 => Some(2),
            DataType::Int32 | DataType::UInt32 | DataType::Float32 => Some(4),
            DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Time
            | DataType::Interval => Some(8),
            DataType::Date => Some(4),
            DataType::Decimal | DataType::Uuid | DataType::Decimal128 => Some(16), // i128/16-byte payloads
            DataType::String
            | DataType::Binary
            | DataType::Json
            | DataType::Array
            | DataType::Blob => None,
            DataType::Float16Vector
            | DataType::Float32Vector
            | DataType::BFloat16Vector
            | DataType::Int8Vector
            | DataType::UInt8Vector
            | DataType::Bit1Vector
            | DataType::TurboQuant2Vector
            | DataType::TurboQuant3Vector
            | DataType::TurboQuant4Vector => None,
        }
    }

    /// Check if this type is numeric
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal
        )
    }

    /// Check if this type is variable-length
    pub fn is_variable_length(&self) -> bool {
        matches!(
            self,
            DataType::String | DataType::Binary | DataType::Json | DataType::Array | DataType::Blob
        )
    }

    /// Check if this type is a decimal type
    pub fn is_decimal(&self) -> bool {
        matches!(self, DataType::Decimal)
    }

    /// True for the timestamp/date/time/interval family.
    pub(crate) fn is_time_family(&self) -> bool {
        matches!(
            self,
            DataType::Timestamp
                | DataType::TimestampTz
                | DataType::Date
                | DataType::Time
                | DataType::Interval
        )
    }

    /// Convert from SQL type string (for compatibility with DuckDB types).
    ///
    /// Unknown names return `None`; callers turn that into an explicit error
    /// instead of silently degrading the column to text (V5 S2 ⑥).
    pub fn from_sql_type(sql_type: &str) -> Option<Self> {
        let sql_type = sql_type.to_uppercase();
        let data_type = match sql_type.as_str() {
            "BOOLEAN" | "BOOL" => DataType::Bool,
            "TINYINT" | "INT1" => DataType::Int8,
            "SMALLINT" | "INT2" => DataType::Int16,
            "INTEGER" | "INT" | "INT4" => DataType::Int32,
            "BIGINT" | "INT8" | "LONG" => DataType::Int64,
            "UTINYINT" => DataType::UInt8,
            "USMALLINT" => DataType::UInt16,
            "UINTEGER" => DataType::UInt32,
            "UBIGINT" => DataType::UInt64,
            "FLOAT" | "REAL" | "FLOAT4" => DataType::Float32,
            "DOUBLE" | "FLOAT8" => DataType::Float64,
            "VARCHAR" | "TEXT" | "STRING" | "CHAR" => DataType::String,
            "BLOB" | "LARGEBINARY" | "LARGE_BINARY" | "LONGBLOB" => DataType::Blob,
            "BYTEA" | "BINARY" | "VARBINARY" | "BYTES" => DataType::Binary,
            "JSON" => DataType::Json,
            "DECIMAL" | "NUMERIC" => DataType::Decimal,
            "TIMESTAMP" | "DATETIME" => DataType::Timestamp,
            "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => DataType::TimestampTz,
            "DATE" => DataType::Date,
            "TIME" => DataType::Time,
            "INTERVAL" => DataType::Interval,
            "FLOAT32_VECTOR" | "F32_VECTOR" => DataType::Float32Vector,
            "FLOAT16_VECTOR" | "FLOAT16VECTOR" | "F16_VECTOR" => DataType::Float16Vector,
            "BFLOAT16_VECTOR" | "BF16_VECTOR" => DataType::BFloat16Vector,
            "INT8_VECTOR" | "I8_VECTOR" => DataType::Int8Vector,
            "UINT8_VECTOR" | "U8_VECTOR" => DataType::UInt8Vector,
            "BIT1_VECTOR" | "BINARY1_VECTOR" => DataType::Bit1Vector,
            "TURBOQUANT2_VECTOR" | "TQ2_VECTOR" => DataType::TurboQuant2Vector,
            "TURBOQUANT3_VECTOR" | "TQ3_VECTOR" => DataType::TurboQuant3Vector,
            "TURBOQUANT4_VECTOR" | "TQ4_VECTOR" => DataType::TurboQuant4Vector,
            _ => return None,
        };
        Some(data_type)
    }

    /// Convert to SQL type string
    pub fn to_sql_type(&self) -> &'static str {
        match self {
            DataType::Null => "NULL",
            DataType::Bool => "BOOLEAN",
            DataType::Int8 => "TINYINT",
            DataType::Int16 => "SMALLINT",
            DataType::Int32 => "INTEGER",
            DataType::Int64 => "BIGINT",
            DataType::UInt8 => "UTINYINT",
            DataType::UInt16 => "USMALLINT",
            DataType::UInt32 => "UINTEGER",
            DataType::UInt64 => "UBIGINT",
            DataType::Float32 => "FLOAT",
            DataType::Float64 => "DOUBLE",
            DataType::String => "VARCHAR",
            DataType::Binary => "BINARY",
            DataType::Blob => "BLOB",
            DataType::Json => "JSON",
            DataType::Timestamp => "TIMESTAMP",
            DataType::TimestampTz => "TIMESTAMPTZ",
            DataType::Date => "DATE",
            DataType::Time => "TIME",
            DataType::Interval => "INTERVAL",
            DataType::Array => "ARRAY",
            DataType::Decimal => "DECIMAL",
            DataType::Uuid => "UUID",
            DataType::Decimal128 => "DECIMAL128",
            DataType::Float16Vector => "FLOAT16_VECTOR",
            DataType::Float32Vector => "FLOAT32_VECTOR",
            DataType::BFloat16Vector => "BFLOAT16_VECTOR",
            DataType::Int8Vector => "INT8_VECTOR",
            DataType::UInt8Vector => "UINT8_VECTOR",
            DataType::Bit1Vector => "BIT1_VECTOR",
            DataType::TurboQuant2Vector => "TURBOQUANT2_VECTOR",
            DataType::TurboQuant3Vector => "TURBOQUANT3_VECTOR",
            DataType::TurboQuant4Vector => "TURBOQUANT4_VECTOR",
        }
    }
}

impl Default for DataType {
    fn default() -> Self {
        DataType::Null
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_sql_type())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_sql_type() {
        assert_eq!(DataType::from_sql_type("BIGINT"), Some(DataType::Int64));
        assert_eq!(DataType::from_sql_type("VARCHAR"), Some(DataType::String));
        assert_eq!(DataType::from_sql_type("DOUBLE"), Some(DataType::Float64));
        assert_eq!(DataType::from_sql_type("BOOLEAN"), Some(DataType::Bool));
        assert_eq!(DataType::from_sql_type("TIMESTAMPTZ"), Some(DataType::TimestampTz));
        assert_eq!(DataType::from_sql_type("TIME"), Some(DataType::Time));
        assert_eq!(DataType::from_sql_type("INTERVAL"), Some(DataType::Interval));
        assert_eq!(DataType::from_sql_type("NO_SUCH_TYPE"), None);
    }

    #[test]
    fn test_fixed_size() {
        assert_eq!(DataType::Int64.fixed_size(), Some(8));
        assert_eq!(DataType::String.fixed_size(), None);
    }
}
