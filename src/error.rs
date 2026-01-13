use thiserror::Error;

#[derive(Error, Debug)]
pub enum BarqError {
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

    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),

    #[error("Primary key violation: duplicate key '{0}'")]
    PrimaryKeyViolation(String),

    #[error("Not null violation: column '{0}' cannot be null")]
    NotNullViolation(String),

    #[error("Unique constraint violation: duplicate value in column '{0}'")]
    UniqueViolation(String),

    #[error("Transaction error: {0}")]
    TransactionError(String),

    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Authentication failed: {0}")]
    AuthError(String),

    #[error("Unsupported SQL statement: {0}")]
    UnsupportedStatement(String),

    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(String),

    #[error("Empty query")]
    EmptyQuery,

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type BarqResult<T> = Result<T, BarqError>;

impl From<BarqError> for pgwire::error::PgWireError {
    fn from(e: BarqError) -> Self {
        pgwire::error::PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
            "ERROR".to_string(),
            "XX000".to_string(),
            e.to_string(),
        )))
    }
}
