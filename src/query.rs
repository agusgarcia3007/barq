use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use sqlparser::ast::{Expr, SetExpr, Statement, TableFactor, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::{DbError, DbResult};
use crate::storage::{BatchBuilder, Catalog, TableSchema};

pub struct QueryEngine {
    catalog: Catalog,
}

impl QueryEngine {
    pub fn new() -> Self {
        Self {
            catalog: Catalog::new(),
        }
    }

    pub fn execute(&mut self, sql: &str) -> DbResult<QueryResult> {
        let dialect = GenericDialect {};
        let statements =
            Parser::parse_sql(&dialect, sql).map_err(|e| DbError::ParseError(e.to_string()))?;

        let statement = statements.into_iter().next().ok_or(DbError::EmptyQuery)?;

        match statement {
            Statement::CreateTable(create) => self.execute_create_table(create),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Query(query) => self.execute_query(*query),
            other => Err(DbError::UnsupportedStatement(format!("{:?}", other))),
        }
    }

    fn execute_create_table(
        &mut self,
        create: sqlparser::ast::CreateTable,
    ) -> DbResult<QueryResult> {
        let table_name = create.name.to_string();
        let columns: Vec<(String, DataType)> = create
            .columns
            .iter()
            .map(|col| {
                let dtype = sql_type_to_arrow(&col.data_type);
                (col.name.value.clone(), dtype)
            })
            .collect();

        let schema = TableSchema {
            name: table_name.clone(),
            columns,
        };

        self.catalog.create_table(schema)?;
        Ok(QueryResult::Success(format!("Created table {}", table_name)))
    }

    fn execute_insert(&mut self, insert: sqlparser::ast::Insert) -> DbResult<QueryResult> {
        let table_name = insert.table.to_string();
        let table = self.catalog.get_table(&table_name)?;
        let schema = table.schema().clone();

        let mut builder = BatchBuilder::new(&schema);

        if let Some(source) = insert.source {
            if let SetExpr::Values(values) = *source.body {
                for row in values.rows {
                    for (i, expr) in row.into_iter().enumerate() {
                        let (col_name, col_type) = &schema.columns[i];
                        self.append_value(&mut builder, col_name, col_type, expr)?;
                    }
                }
            }
        }

        let batch = builder.finish()?;
        let row_count = batch.num_rows();

        let table = self.catalog.get_table_mut(&table_name)?;
        table.insert_batch(batch)?;

        Ok(QueryResult::Success(format!(
            "Inserted {} row(s)",
            row_count
        )))
    }

    fn execute_query(&self, query: sqlparser::ast::Query) -> DbResult<QueryResult> {
        let select = match *query.body {
            SetExpr::Select(select) => select,
            _ => {
                return Err(DbError::UnsupportedStatement(
                    "Only SELECT queries supported".to_string(),
                ))
            }
        };

        let table_name = select
            .from
            .first()
            .and_then(|from| match &from.relation {
                TableFactor::Table { name, .. } => Some(name.to_string()),
                _ => None,
            })
            .ok_or_else(|| DbError::ParseError("Missing FROM clause".to_string()))?;

        let table = self.catalog.get_table(&table_name)?;

        let batches: Vec<RecordBatch> = table
            .scan()
            .into_iter()
            .map(|arc| (*arc).clone())
            .collect();

        Ok(QueryResult::Batches(batches))
    }

    fn append_value(
        &self,
        builder: &mut BatchBuilder,
        column: &str,
        dtype: &DataType,
        expr: Expr,
    ) -> DbResult<()> {
        match (dtype, expr) {
            (DataType::Int32, Expr::Value(Value::Number(n, _))) => {
                let value: i32 = n.parse().map_err(|_| DbError::InvalidValue {
                    column: column.to_string(),
                    value: n,
                })?;
                builder.append_int(column, value)
            }
            (DataType::Utf8, Expr::Value(Value::SingleQuotedString(s))) => {
                builder.append_string(column, &s)
            }
            (dtype, expr) => Err(DbError::TypeMismatch {
                expected: format!("{:?}", dtype),
                actual: format!("{:?}", expr),
            }),
        }
    }
}

pub enum QueryResult {
    Success(String),
    Batches(Vec<RecordBatch>),
}

impl QueryResult {
    pub fn display(&self) -> DbResult<()> {
        match self {
            QueryResult::Success(msg) => {
                println!("{}", msg);
                Ok(())
            }
            QueryResult::Batches(batches) => {
                if batches.is_empty() {
                    println!("(empty result set)");
                } else {
                    print_batches(batches).map_err(DbError::from)?;
                }
                Ok(())
            }
        }
    }
}

fn sql_type_to_arrow(sql_type: &sqlparser::ast::DataType) -> DataType {
    match sql_type {
        sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => DataType::Int32,
        sqlparser::ast::DataType::Varchar(_) | sqlparser::ast::DataType::Text => DataType::Utf8,
        _ => DataType::Utf8,
    }
}
