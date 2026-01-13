use arrow::datatypes::DataType as ArrowDataType;
use pgwire::api::Type as PgType;
use serde::{Deserialize, Serialize};
use sqlparser::ast::DataType as SqlDataType;

/// Internal representation of data types
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BarqType {
    Bool,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Text,
    Varchar(Option<u32>),
    Timestamp,
    Date,
    Bytea,
    Uuid,
    Json,
    Jsonb,
    Null,
}

impl BarqType {
    pub fn from_sql(sql_type: &SqlDataType) -> Self {
        match sql_type {
            SqlDataType::Boolean => BarqType::Bool,
            SqlDataType::SmallInt(_) => BarqType::Int16,
            SqlDataType::Int(_) | SqlDataType::Integer(_) => BarqType::Int32,
            SqlDataType::BigInt(_) => BarqType::Int64,
            SqlDataType::Real => BarqType::Float32,
            SqlDataType::Float(_) | SqlDataType::Double | SqlDataType::DoublePrecision => {
                BarqType::Float64
            }
            SqlDataType::Text => BarqType::Text,
            SqlDataType::Varchar(len) => BarqType::Varchar(len.map(|l| match l {
                sqlparser::ast::CharacterLength::IntegerLength { length, .. } => length as u32,
                sqlparser::ast::CharacterLength::Max => u32::MAX,
            })),
            SqlDataType::Char(len) => BarqType::Varchar(len.map(|l| match l {
                sqlparser::ast::CharacterLength::IntegerLength { length, .. } => length as u32,
                sqlparser::ast::CharacterLength::Max => u32::MAX,
            })),
            SqlDataType::Timestamp(_, _) => BarqType::Timestamp,
            SqlDataType::Date => BarqType::Date,
            SqlDataType::Bytea => BarqType::Bytea,
            SqlDataType::Uuid => BarqType::Uuid,
            SqlDataType::JSON => BarqType::Json,
            SqlDataType::JSONB => BarqType::Jsonb,
            _ => BarqType::Text, // Default fallback
        }
    }

    pub fn to_arrow(&self) -> ArrowDataType {
        match self {
            BarqType::Bool => ArrowDataType::Boolean,
            BarqType::Int16 => ArrowDataType::Int16,
            BarqType::Int32 => ArrowDataType::Int32,
            BarqType::Int64 => ArrowDataType::Int64,
            BarqType::Float32 => ArrowDataType::Float32,
            BarqType::Float64 => ArrowDataType::Float64,
            BarqType::Text | BarqType::Varchar(_) => ArrowDataType::Utf8,
            BarqType::Timestamp => ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            BarqType::Date => ArrowDataType::Date32,
            BarqType::Bytea => ArrowDataType::Binary,
            BarqType::Uuid => ArrowDataType::Utf8,
            BarqType::Json | BarqType::Jsonb => ArrowDataType::Utf8,
            BarqType::Null => ArrowDataType::Null,
        }
    }

    pub fn to_pg(&self) -> PgType {
        match self {
            BarqType::Bool => PgType::BOOL,
            BarqType::Int16 => PgType::INT2,
            BarqType::Int32 => PgType::INT4,
            BarqType::Int64 => PgType::INT8,
            BarqType::Float32 => PgType::FLOAT4,
            BarqType::Float64 => PgType::FLOAT8,
            BarqType::Text | BarqType::Varchar(_) => PgType::VARCHAR,
            BarqType::Timestamp => PgType::TIMESTAMP,
            BarqType::Date => PgType::DATE,
            BarqType::Bytea => PgType::BYTEA,
            BarqType::Uuid => PgType::UUID,
            BarqType::Json => PgType::JSON,
            BarqType::Jsonb => PgType::JSONB,
            BarqType::Null => PgType::UNKNOWN,
        }
    }

    pub fn from_pg(pg_type: &PgType) -> Self {
        match *pg_type {
            PgType::BOOL => BarqType::Bool,
            PgType::INT2 => BarqType::Int16,
            PgType::INT4 => BarqType::Int32,
            PgType::INT8 => BarqType::Int64,
            PgType::FLOAT4 => BarqType::Float32,
            PgType::FLOAT8 => BarqType::Float64,
            PgType::VARCHAR | PgType::TEXT | PgType::CHAR | PgType::BPCHAR | PgType::NAME => BarqType::Text,
            PgType::TIMESTAMP | PgType::TIMESTAMPTZ => BarqType::Timestamp,
            PgType::DATE => BarqType::Date,
            PgType::BYTEA => BarqType::Bytea,
            PgType::UUID => BarqType::Uuid,
            PgType::JSON => BarqType::Json,
            PgType::JSONB => BarqType::Jsonb,
            _ => BarqType::Text,
        }
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            BarqType::Int16 | BarqType::Int32 | BarqType::Int64 | BarqType::Float32 | BarqType::Float64
        )
    }

    pub fn is_text(&self) -> bool {
        matches!(self, BarqType::Text | BarqType::Varchar(_))
    }
}

/// Represents a value in the database
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Text(String),
    Timestamp(i64), // microseconds since epoch
    Date(i32),      // days since epoch
    Bytea(Vec<u8>),
    Uuid(String),
    Json(String),
}

impl Value {
    pub fn get_type(&self) -> BarqType {
        match self {
            Value::Null => BarqType::Null,
            Value::Bool(_) => BarqType::Bool,
            Value::Int16(_) => BarqType::Int16,
            Value::Int32(_) => BarqType::Int32,
            Value::Int64(_) => BarqType::Int64,
            Value::Float32(_) => BarqType::Float32,
            Value::Float64(_) => BarqType::Float64,
            Value::Text(_) => BarqType::Text,
            Value::Timestamp(_) => BarqType::Timestamp,
            Value::Date(_) => BarqType::Date,
            Value::Bytea(_) => BarqType::Bytea,
            Value::Uuid(_) => BarqType::Uuid,
            Value::Json(_) => BarqType::Json,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn to_string_repr(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int16(i) => i.to_string(),
            Value::Int32(i) => i.to_string(),
            Value::Int64(i) => i.to_string(),
            Value::Float32(f) => f.to_string(),
            Value::Float64(f) => f.to_string(),
            Value::Text(s) => s.clone(),
            Value::Timestamp(ts) => chrono::DateTime::from_timestamp_micros(*ts)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                .unwrap_or_else(|| ts.to_string()),
            Value::Date(d) => chrono::NaiveDate::from_num_days_from_ce_opt(*d)
                .map(|date| date.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| d.to_string()),
            Value::Bytea(b) => format!("\\x{}", hex::encode(b)),
            Value::Uuid(u) => u.clone(),
            Value::Json(j) => j.clone(),
        }
    }
}

// Simple hex encoding for bytea display
mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{:02x}", b)).collect()
    }
}
