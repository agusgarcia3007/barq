use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("SQL parse error: {0}")]
    ParseError(String),

    #[error("Table '{0}' not found")]
    TableNotFound(String),

    #[error("Table '{0}' already exists")]
    TableAlreadyExists(String),

    #[error("Column '{0}' not found in table '{1}'")]
    ColumnNotFound(String, String),

    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Invalid value '{value}' for column '{column}'")]
    InvalidValue { column: String, value: String },

    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Unsupported SQL statement: {0}")]
    UnsupportedStatement(String),

    #[error("Empty query")]
    EmptyQuery,
}

pub type DbResult<T> = Result<T, DbError>;
