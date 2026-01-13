use arrow::array::{Array, Int32Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use prettytable::{Cell, Row, Table};
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
            Statement::CreateTable { name, columns, .. } => {
                self.execute_create_table(name.to_string(), columns)
            }
            Statement::Insert { table_name, source, .. } => {
                self.execute_insert(table_name.to_string(), source)
            }
            Statement::Query(query) => self.execute_query(*query),
            other => Err(DbError::UnsupportedStatement(format!("{:?}", other))),
        }
    }

    fn execute_create_table(
        &mut self,
        table_name: String,
        columns: Vec<sqlparser::ast::ColumnDef>,
    ) -> DbResult<QueryResult> {
        let cols: Vec<(String, DataType)> = columns
            .iter()
            .map(|col| {
                let dtype = sql_type_to_arrow(&col.data_type);
                (col.name.value.clone(), dtype)
            })
            .collect();

        let schema = TableSchema {
            name: table_name.clone(),
            columns: cols,
        };

        self.catalog.create_table(schema)?;
        Ok(QueryResult::Success(format!("Created table {}", table_name)))
    }

    fn execute_insert(
        &mut self,
        table_name: String,
        source: Option<Box<sqlparser::ast::Query>>,
    ) -> DbResult<QueryResult> {
        let table = self.catalog.get_table(&table_name)?;
        let schema = table.schema().clone();

        let mut builder = BatchBuilder::new(&schema);

        if let Some(query) = source {
            if let SetExpr::Values(values) = *query.body {
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
                    print_batches(batches);
                }
                Ok(())
            }
        }
    }
}

fn print_batches(batches: &[RecordBatch]) {
    if batches.is_empty() {
        return;
    }

    let mut table = Table::new();
    let schema = batches[0].schema();

    // Header row
    let headers: Vec<Cell> = schema
        .fields()
        .iter()
        .map(|f| Cell::new(f.name()))
        .collect();
    table.add_row(Row::new(headers));

    // Data rows
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let cells: Vec<Cell> = (0..batch.num_columns())
                .map(|col_idx| {
                    let col = batch.column(col_idx);
                    let value = get_cell_value(col.as_ref(), row_idx);
                    Cell::new(&value)
                })
                .collect();
            table.add_row(Row::new(cells));
        }
    }

    table.printstd();
}

fn get_cell_value(array: &dyn Array, idx: usize) -> String {
    if array.is_null(idx) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            arr.value(idx).to_string()
        }
        _ => "?".to_string(),
    }
}

fn sql_type_to_arrow(sql_type: &sqlparser::ast::DataType) -> DataType {
    match sql_type {
        sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => DataType::Int32,
        sqlparser::ast::DataType::Varchar(_) | sqlparser::ast::DataType::Text => DataType::Utf8,
        _ => DataType::Utf8,
    }
}
