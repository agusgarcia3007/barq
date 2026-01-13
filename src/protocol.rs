use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType as ArrowDataType, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream;
use pgwire::api::auth::md5pass::MakeMd5PasswordAuthStartupHandler;
use pgwire::api::auth::{AuthSource, LoginInfo, Password};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, MakeHandler, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use tokio::sync::Mutex;

use crate::catalog::Catalog;
use crate::executor::{Executor, QueryResult};
use crate::storage::StorageEngine;

/// Authentication source for Barq
pub struct BarqAuthSource {
    users: std::collections::HashMap<String, String>,
}

impl BarqAuthSource {
    pub fn new() -> Self {
        let mut users = std::collections::HashMap::new();
        // Default credentials
        users.insert("barq".to_string(), "barq".to_string());
        users.insert("postgres".to_string(), "postgres".to_string());
        Self { users }
    }

    pub fn add_user(&mut self, username: &str, password: &str) {
        self.users.insert(username.to_string(), password.to_string());
    }
}

#[async_trait]
impl AuthSource for BarqAuthSource {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        let username = login_info.user().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "FATAL".to_string(),
                "28000".to_string(),
                "No username provided".to_string(),
            )))
        })?;

        let password = self.users.get(username).ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "FATAL".to_string(),
                "28P01".to_string(),
                format!("Password authentication failed for user \"{}\"", username),
            )))
        })?;

        Ok(Password::new(Some(login_info.salt().to_vec()), password.as_bytes().to_vec()))
    }
}

/// PostgreSQL protocol handler for Barq
pub struct BarqHandler {
    catalog: Arc<Catalog>,
    storage: Arc<StorageEngine>,
    executor: Arc<Mutex<Executor>>,
    portal_store: Arc<MemPortalStore<String>>,
}

impl BarqHandler {
    pub fn new(catalog: Arc<Catalog>, storage: Arc<StorageEngine>) -> Self {
        let executor = Executor::new(catalog.clone(), storage.clone());
        Self {
            catalog,
            storage,
            executor: Arc::new(Mutex::new(executor)),
            portal_store: Arc::new(MemPortalStore::new()),
        }
    }

    fn schema_to_field_info(&self, schema: &Schema) -> Vec<FieldInfo> {
        schema
            .fields()
            .iter()
            .map(|field| {
                let pg_type = arrow_to_pg_type(field.data_type());
                FieldInfo::new(field.name().clone(), None, None, pg_type, Format::Text)
            })
            .collect()
    }

    fn encode_batch(&self, batch: &RecordBatch, format: &Format) -> PgWireResult<Vec<DataRowEncoder>> {
        let mut rows = Vec::with_capacity(batch.num_rows());
        let schema = batch.schema();

        for row_idx in 0..batch.num_rows() {
            let mut encoder = DataRowEncoder::new(Arc::new(
                schema
                    .fields()
                    .iter()
                    .map(|f| arrow_to_pg_type(f.data_type()))
                    .collect(),
            ));

            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                self.encode_value(&mut encoder, array.as_ref(), row_idx, format)?;
            }

            rows.push(encoder);
        }

        Ok(rows)
    }

    fn encode_value(
        &self,
        encoder: &mut DataRowEncoder,
        array: &dyn Array,
        idx: usize,
        _format: &Format,
    ) -> PgWireResult<()> {
        if array.is_null(idx) {
            encoder.encode_field(&None::<&str>)?;
            return Ok(());
        }

        match array.data_type() {
            ArrowDataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                encoder.encode_field(&arr.value(idx))?;
            }
            ArrowDataType::Int16 => {
                let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
                encoder.encode_field(&arr.value(idx))?;
            }
            ArrowDataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                encoder.encode_field(&arr.value(idx))?;
            }
            ArrowDataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                encoder.encode_field(&arr.value(idx))?;
            }
            ArrowDataType::Float32 => {
                let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
                encoder.encode_field(&arr.value(idx))?;
            }
            ArrowDataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                encoder.encode_field(&arr.value(idx))?;
            }
            ArrowDataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                encoder.encode_field(&arr.value(idx))?;
            }
            ArrowDataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                encoder.encode_field(&arr.value(idx))?;
            }
            ArrowDataType::Binary => {
                let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                encoder.encode_field(&arr.value(idx))?;
            }
            ArrowDataType::Timestamp(_, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                let ts = arr.value(idx);
                let dt = chrono::DateTime::from_timestamp_micros(ts)
                    .map(|d| d.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                    .unwrap_or_else(|| ts.to_string());
                encoder.encode_field(&dt)?;
            }
            ArrowDataType::Date32 => {
                let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
                let days = arr.value(idx);
                let date = chrono::NaiveDate::from_num_days_from_ce_opt(days)
                    .map(|d| d.format("%Y-%m-%d").to_string())
                    .unwrap_or_else(|| days.to_string());
                encoder.encode_field(&date)?;
            }
            _ => {
                // Default: encode as string
                encoder.encode_field(&format!("{:?}", array))?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for BarqHandler {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send,
    {
        tracing::debug!("Executing query: {}", query);

        // Handle empty queries
        if query.trim().is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        // Split multiple statements
        let statements: Vec<&str> = query.split(';').filter(|s| !s.trim().is_empty()).collect();
        let mut responses = Vec::new();

        for stmt in statements {
            let executor = self.executor.lock().await;
            match executor.execute(stmt) {
                Ok(QueryResult::Command { tag, rows_affected }) => {
                    responses.push(Response::Execution(
                        Tag::new(&tag).with_rows(rows_affected as usize),
                    ));
                }
                Ok(QueryResult::Rows { schema, batches }) => {
                    let field_info = self.schema_to_field_info(&schema);

                    let mut all_rows = Vec::new();
                    for batch in &batches {
                        let rows = self.encode_batch(batch, &Format::Text)?;
                        all_rows.extend(rows);
                    }

                    let data_row_stream = stream::iter(all_rows.into_iter().map(Ok));

                    responses.push(Response::Query(QueryResponse::new(
                        Arc::new(field_info),
                        data_row_stream,
                    )));
                }
                Ok(QueryResult::Empty) => {
                    responses.push(Response::Execution(Tag::new("OK")));
                }
                Err(e) => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_string(),
                        "XX000".to_string(),
                        e.to_string(),
                    ))));
                }
            }
        }

        Ok(responses)
    }
}

#[async_trait]
impl ExtendedQueryHandler for BarqHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser::new())
    }

    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send,
    {
        let query = portal.statement().statement();
        tracing::debug!("Extended query: {}", query);

        let executor = self.executor.lock().await;
        match executor.execute(query) {
            Ok(QueryResult::Command { tag, rows_affected }) => {
                Ok(Response::Execution(Tag::new(&tag).with_rows(rows_affected as usize)))
            }
            Ok(QueryResult::Rows { schema, batches }) => {
                let field_info = self.schema_to_field_info(&schema);

                let mut all_rows = Vec::new();
                for batch in &batches {
                    let format = portal.result_column_format();
                    let default_format = Format::Text;
                    let rows = self.encode_batch(batch, format.first().unwrap_or(&default_format))?;
                    all_rows.extend(rows);
                }

                let data_row_stream = stream::iter(all_rows.into_iter().map(Ok));

                Ok(Response::Query(QueryResponse::new(
                    Arc::new(field_info),
                    data_row_stream,
                )))
            }
            Ok(QueryResult::Empty) => Ok(Response::Execution(Tag::new("OK"))),
            Err(e) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                e.to_string(),
            )))),
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send,
    {
        // For now, return empty parameter info
        // TODO: Parse query to determine parameters
        Ok(DescribeStatementResponse::new(vec![], vec![]))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send,
    {
        let query = portal.statement().statement();

        // Try to execute to get schema
        let executor = self.executor.lock().await;
        match executor.execute(query) {
            Ok(QueryResult::Rows { schema, .. }) => {
                let field_info = self.schema_to_field_info(&schema);
                Ok(DescribePortalResponse::new(field_info))
            }
            _ => Ok(DescribePortalResponse::new(vec![])),
        }
    }
}

/// Handler factory
pub struct MakeBarqHandler {
    catalog: Arc<Catalog>,
    storage: Arc<StorageEngine>,
}

impl MakeBarqHandler {
    pub fn new(catalog: Arc<Catalog>, storage: Arc<StorageEngine>) -> Self {
        Self { catalog, storage }
    }
}

impl MakeHandler for MakeBarqHandler {
    type Handler = Arc<BarqHandler>;

    fn make(&self) -> Self::Handler {
        Arc::new(BarqHandler::new(self.catalog.clone(), self.storage.clone()))
    }
}

/// Convert Arrow data type to PostgreSQL type
fn arrow_to_pg_type(arrow_type: &ArrowDataType) -> Type {
    match arrow_type {
        ArrowDataType::Boolean => Type::BOOL,
        ArrowDataType::Int16 => Type::INT2,
        ArrowDataType::Int32 => Type::INT4,
        ArrowDataType::Int64 => Type::INT8,
        ArrowDataType::Float32 => Type::FLOAT4,
        ArrowDataType::Float64 => Type::FLOAT8,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Type::VARCHAR,
        ArrowDataType::Binary | ArrowDataType::LargeBinary => Type::BYTEA,
        ArrowDataType::Timestamp(_, _) => Type::TIMESTAMP,
        ArrowDataType::Date32 | ArrowDataType::Date64 => Type::DATE,
        _ => Type::VARCHAR,
    }
}
