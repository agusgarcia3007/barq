use std::sync::Arc;

use arrow::array::*;
use arrow::compute::{concat_batches, filter_record_batch, sort_to_indices, take, and, or, SortOptions};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_ord::cmp::{eq, neq, lt, lt_eq, gt, gt_eq};
use sqlparser::ast::{
    BinaryOperator, ColumnDef as SqlColumnDef, Expr, Ident, ObjectName, OrderByExpr,
    Query, Select, SelectItem, SetExpr, Statement, TableConstraint, TableFactor,
    TableWithJoins, Value as SqlValue, Values,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::catalog::{Catalog, ColumnDef, TableDef};
use crate::error::{BarqError, BarqResult};
use crate::storage::{BatchBuilder, StorageEngine};
use crate::types::{BarqType, Value};

/// Query execution result
pub enum QueryResult {
    /// Command completed successfully with a message
    Command { tag: String, rows_affected: u64 },
    /// Query returned rows
    Rows {
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    },
    /// Empty result (e.g., BEGIN, COMMIT)
    Empty,
}

/// SQL query executor
pub struct Executor {
    catalog: Arc<Catalog>,
    storage: Arc<StorageEngine>,
}

impl Executor {
    pub fn new(catalog: Arc<Catalog>, storage: Arc<StorageEngine>) -> Self {
        Self { catalog, storage }
    }

    pub fn execute(&self, sql: &str) -> BarqResult<QueryResult> {
        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| BarqError::ParseError(e.to_string()))?;

        if statements.is_empty() {
            return Err(BarqError::EmptyQuery);
        }

        // Execute first statement
        self.execute_statement(&statements[0])
    }

    pub fn execute_statement(&self, stmt: &Statement) -> BarqResult<QueryResult> {
        match stmt {
            Statement::CreateTable(create) => {
                self.execute_create_table(&create.name, &create.columns, &create.constraints)
            }
            Statement::Drop { object_type, names, if_exists, .. } => {
                self.execute_drop(object_type, names, *if_exists)
            }
            Statement::Insert(insert) => {
                self.execute_insert(&insert.table_name, &insert.columns, insert.source.as_ref())
            }
            Statement::Query(query) => self.execute_query(query),
            Statement::Update { table, assignments, selection, .. } => {
                self.execute_update(table, assignments, selection.as_ref())
            }
            Statement::Delete(delete) => {
                self.execute_delete(&delete.from, delete.selection.as_ref())
            }
            Statement::Truncate { table_names, .. } => self.execute_truncate(table_names),
            Statement::AlterTable { name, operations, .. } => {
                self.execute_alter_table(name, operations)
            }
            Statement::CreateIndex(create_index) => self.execute_create_index(create_index),
            Statement::StartTransaction { .. } => Ok(QueryResult::Command {
                tag: "BEGIN".to_string(),
                rows_affected: 0,
            }),
            Statement::Commit { .. } => Ok(QueryResult::Command {
                tag: "COMMIT".to_string(),
                rows_affected: 0,
            }),
            Statement::Rollback { .. } => Ok(QueryResult::Command {
                tag: "ROLLBACK".to_string(),
                rows_affected: 0,
            }),
            Statement::SetVariable { .. } => Ok(QueryResult::Command {
                tag: "SET".to_string(),
                rows_affected: 0,
            }),
            Statement::ShowVariable { .. } => self.execute_show_variable(stmt),
            _ => Err(BarqError::UnsupportedStatement(format!("{:?}", stmt))),
        }
    }

    fn execute_create_table(
        &self,
        name: &ObjectName,
        columns: &[SqlColumnDef],
        constraints: &[TableConstraint],
    ) -> BarqResult<QueryResult> {
        let table_name = name.to_string();

        // Parse columns
        let mut cols: Vec<ColumnDef> = columns
            .iter()
            .map(|col| {
                let data_type = BarqType::from_sql(&col.data_type);
                let nullable = !col.options.iter().any(|opt| {
                    matches!(
                        opt.option,
                        sqlparser::ast::ColumnOption::NotNull
                    )
                });
                let is_primary_key = col.options.iter().any(|opt| {
                    matches!(
                        opt.option,
                        sqlparser::ast::ColumnOption::Unique { is_primary: true, .. }
                    )
                });
                let is_unique = col.options.iter().any(|opt| {
                    matches!(
                        opt.option,
                        sqlparser::ast::ColumnOption::Unique { is_primary: false, .. }
                    )
                });
                let default = col.options.iter().find_map(|opt| {
                    if let sqlparser::ast::ColumnOption::Default(expr) = &opt.option {
                        Some(expr.to_string())
                    } else {
                        None
                    }
                });

                ColumnDef {
                    name: col.name.value.clone(),
                    data_type,
                    nullable: nullable && !is_primary_key,
                    default,
                    is_primary_key,
                    is_unique,
                }
            })
            .collect();

        // Parse table constraints
        let mut primary_key: Option<Vec<String>> = None;
        let mut unique_constraints: Vec<Vec<String>> = Vec::new();

        for constraint in constraints {
            match constraint {
                TableConstraint::PrimaryKey { columns, .. } => {
                    let pk_cols: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();
                    for pk_col in &pk_cols {
                        if let Some(col) = cols.iter_mut().find(|c| &c.name == pk_col) {
                            col.is_primary_key = true;
                            col.nullable = false;
                        }
                    }
                    primary_key = Some(pk_cols);
                }
                TableConstraint::Unique { columns, .. } => {
                    let uniq_cols: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();
                    unique_constraints.push(uniq_cols);
                }
                _ => {}
            }
        }

        let table_def = TableDef {
            name: table_name.clone(),
            schema: "public".to_string(),
            columns: cols,
            primary_key,
            unique_constraints,
            created_at: chrono::Utc::now().timestamp_micros(),
        };

        self.catalog.create_table(table_def.clone())?;
        self.storage.create_table(table_def)?;

        Ok(QueryResult::Command {
            tag: "CREATE TABLE".to_string(),
            rows_affected: 0,
        })
    }

    fn execute_drop(
        &self,
        object_type: &sqlparser::ast::ObjectType,
        names: &[ObjectName],
        if_exists: bool,
    ) -> BarqResult<QueryResult> {
        match object_type {
            sqlparser::ast::ObjectType::Table => {
                for name in names {
                    let table_name = name.to_string();
                    match self.storage.drop_table(&table_name) {
                        Ok(_) => {
                            let _ = self.catalog.drop_table(&table_name);
                        }
                        Err(e) => {
                            if !if_exists {
                                return Err(e);
                            }
                        }
                    }
                }
                Ok(QueryResult::Command {
                    tag: "DROP TABLE".to_string(),
                    rows_affected: 0,
                })
            }
            _ => Err(BarqError::UnsupportedStatement(format!(
                "DROP {:?}",
                object_type
            ))),
        }
    }

    fn execute_insert(
        &self,
        table_name: &ObjectName,
        columns: &[Ident],
        source: Option<&Box<Query>>,
    ) -> BarqResult<QueryResult> {
        let name = table_name.to_string();
        let table = self.storage.get_table(&name)?;
        let table_def = table.def();

        // Determine column order
        let col_order: Vec<usize> = if columns.is_empty() {
            (0..table_def.columns.len()).collect()
        } else {
            columns
                .iter()
                .map(|c| {
                    table_def
                        .column_index(&c.value)
                        .ok_or_else(|| BarqError::ColumnNotFound(c.value.clone(), name.clone()))
                })
                .collect::<BarqResult<Vec<_>>>()?
        };

        let mut builder = BatchBuilder::new(table_def);
        let mut row_count = 0;

        if let Some(query) = source {
            if let SetExpr::Values(Values { rows, .. }) = query.body.as_ref() {
                for row in rows {
                    let mut values = vec![Value::Null; table_def.columns.len()];

                    for (i, expr) in row.iter().enumerate() {
                        if i < col_order.len() {
                            let col_idx = col_order[i];
                            let col_type = &table_def.columns[col_idx].data_type;
                            values[col_idx] = self.eval_expr_to_value(expr, col_type)?;
                        }
                    }

                    builder.append_row(&values)?;
                    row_count += 1;
                }
            }
        }

        let batch = builder.finish()?;
        table.insert(batch)?;

        Ok(QueryResult::Command {
            tag: "INSERT".to_string(),
            rows_affected: row_count,
        })
    }

    fn execute_query(&self, query: &Query) -> BarqResult<QueryResult> {
        let select = match query.body.as_ref() {
            SetExpr::Select(select) => select.as_ref(),
            _ => {
                return Err(BarqError::UnsupportedStatement(
                    "Only SELECT queries supported".to_string(),
                ))
            }
        };

        // Handle information_schema queries
        if let Some(from) = select.from.first() {
            if let TableFactor::Table { name, .. } = &from.relation {
                let table_name = name.to_string().to_lowercase();
                if table_name.contains("information_schema") || table_name.contains("pg_catalog") {
                    return self.execute_system_query(select, &table_name);
                }
            }
        }

        // Regular query execution
        let (schema, batches) = self.execute_select(select, &query.order_by, &query.limit)?;

        Ok(QueryResult::Rows { schema, batches })
    }

    fn execute_select(
        &self,
        select: &Select,
        order_by: &[OrderByExpr],
        limit: &Option<Expr>,
    ) -> BarqResult<(Arc<Schema>, Vec<RecordBatch>)> {
        // Get table data
        let from = select.from.first().ok_or_else(|| {
            BarqError::ParseError("SELECT requires FROM clause".to_string())
        })?;

        let table_name = match &from.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => {
                return Err(BarqError::UnsupportedStatement(
                    "Unsupported FROM clause".to_string(),
                ))
            }
        };

        let table = self.storage.get_table(&table_name)?;
        let table_def = table.def();
        let mut batches = table.scan();

        // Apply WHERE clause
        if let Some(selection) = &select.selection {
            batches = self.filter_batches(&batches, selection, table_def)?;
        }

        // Apply JOINs
        for join in &from.joins {
            batches = self.apply_join(&batches, table_def, join)?;
        }

        // Concatenate batches
        let schema = if batches.is_empty() {
            table.schema()
        } else {
            batches[0].schema()
        };

        let combined = if batches.is_empty() {
            RecordBatch::new_empty(schema.clone())
        } else {
            concat_batches(&schema, batches.iter().map(|b| b.as_ref()))?
        };

        // Apply ORDER BY
        let sorted = if !order_by.is_empty() {
            self.sort_batch(&combined, order_by, table_def)?
        } else {
            combined
        };

        // Apply LIMIT
        let limited = if let Some(limit_expr) = limit {
            self.limit_batch(&sorted, limit_expr)?
        } else {
            sorted
        };

        // Project columns
        let (projected_schema, projected) = self.project_columns(&limited, &select.projection, table_def)?;

        Ok((projected_schema, vec![projected]))
    }

    fn filter_batches(
        &self,
        batches: &[Arc<RecordBatch>],
        predicate: &Expr,
        table_def: &TableDef,
    ) -> BarqResult<Vec<Arc<RecordBatch>>> {
        let mut results = Vec::new();

        for batch in batches {
            let mask = self.eval_predicate(batch, predicate, table_def)?;
            let filtered = filter_record_batch(batch, &mask)?;
            if filtered.num_rows() > 0 {
                results.push(Arc::new(filtered));
            }
        }

        Ok(results)
    }

    fn eval_predicate(
        &self,
        batch: &RecordBatch,
        expr: &Expr,
        table_def: &TableDef,
    ) -> BarqResult<BooleanArray> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => {
                        let left_mask = self.eval_predicate(batch, left, table_def)?;
                        let right_mask = self.eval_predicate(batch, right, table_def)?;
                        Ok(and(&left_mask, &right_mask)?)
                    }
                    BinaryOperator::Or => {
                        let left_mask = self.eval_predicate(batch, left, table_def)?;
                        let right_mask = self.eval_predicate(batch, right, table_def)?;
                        Ok(or(&left_mask, &right_mask)?)
                    }
                    BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq => {
                        self.eval_comparison(batch, left, op, right, table_def)
                    }
                    _ => Err(BarqError::UnsupportedStatement(format!(
                        "Unsupported operator: {:?}",
                        op
                    ))),
                }
            }
            Expr::IsNull(inner) => {
                let col_name = self.extract_column_name(inner)?;
                let col_idx = table_def
                    .column_index(&col_name)
                    .ok_or_else(|| BarqError::ColumnNotFound(col_name, table_def.name.clone()))?;
                let array = batch.column(col_idx);

                let mut builder = BooleanBuilder::new();
                for i in 0..array.len() {
                    builder.append_value(array.is_null(i));
                }
                Ok(builder.finish())
            }
            Expr::IsNotNull(inner) => {
                let col_name = self.extract_column_name(inner)?;
                let col_idx = table_def
                    .column_index(&col_name)
                    .ok_or_else(|| BarqError::ColumnNotFound(col_name, table_def.name.clone()))?;
                let array = batch.column(col_idx);

                let mut builder = BooleanBuilder::new();
                for i in 0..array.len() {
                    builder.append_value(!array.is_null(i));
                }
                Ok(builder.finish())
            }
            _ => Err(BarqError::UnsupportedStatement(format!(
                "Unsupported predicate: {:?}",
                expr
            ))),
        }
    }

    fn eval_comparison(
        &self,
        batch: &RecordBatch,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        table_def: &TableDef,
    ) -> BarqResult<BooleanArray> {
        let col_name = self.extract_column_name(left)?;
        let col_idx = table_def
            .column_index(&col_name)
            .ok_or_else(|| BarqError::ColumnNotFound(col_name.clone(), table_def.name.clone()))?;
        let array = batch.column(col_idx);
        let col_type = &table_def.columns[col_idx].data_type;

        let value = self.eval_expr_to_value(right, col_type)?;

        match array.data_type() {
            ArrowDataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                let val = match &value {
                    Value::Int32(v) => *v,
                    Value::Int64(v) => *v as i32,
                    _ => return Err(BarqError::TypeMismatch {
                        expected: "integer".to_string(),
                        actual: format!("{:?}", value),
                    }),
                };
                let scalar = Int32Array::new_scalar(val);
                self.compare_arrays(arr, &scalar, op)
            }
            ArrowDataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                let val = match &value {
                    Value::Int64(v) => *v,
                    Value::Int32(v) => *v as i64,
                    _ => return Err(BarqError::TypeMismatch {
                        expected: "bigint".to_string(),
                        actual: format!("{:?}", value),
                    }),
                };
                let scalar = Int64Array::new_scalar(val);
                self.compare_arrays(arr, &scalar, op)
            }
            ArrowDataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                let val = match &value {
                    Value::Text(s) => s.clone(),
                    _ => value.to_string_repr(),
                };
                let scalar = StringArray::new_scalar(&val);
                self.compare_arrays(arr, &scalar, op)
            }
            ArrowDataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                let val = match &value {
                    Value::Bool(b) => *b,
                    _ => return Err(BarqError::TypeMismatch {
                        expected: "boolean".to_string(),
                        actual: format!("{:?}", value),
                    }),
                };
                let scalar = BooleanArray::new_scalar(val);
                self.compare_arrays(arr, &scalar, op)
            }
            _ => Err(BarqError::UnsupportedFeature(format!(
                "Comparison on type {:?}",
                array.data_type()
            ))),
        }
    }

    fn compare_arrays<T: arrow::array::Datum>(
        &self,
        left: &T,
        right: &T,
        op: &BinaryOperator,
    ) -> BarqResult<BooleanArray> {
        let result = match op {
            BinaryOperator::Eq => eq(left, right)?,
            BinaryOperator::NotEq => neq(left, right)?,
            BinaryOperator::Lt => lt(left, right)?,
            BinaryOperator::LtEq => lt_eq(left, right)?,
            BinaryOperator::Gt => gt(left, right)?,
            BinaryOperator::GtEq => gt_eq(left, right)?,
            _ => {
                return Err(BarqError::UnsupportedStatement(format!(
                    "Unsupported comparison operator: {:?}",
                    op
                )))
            }
        };
        Ok(result)
    }

    fn sort_batch(
        &self,
        batch: &RecordBatch,
        order_by: &[OrderByExpr],
        table_def: &TableDef,
    ) -> BarqResult<RecordBatch> {
        if batch.num_rows() == 0 || order_by.is_empty() {
            return Ok(batch.clone());
        }

        // For now, support single column sort
        let first_order = &order_by[0];
        let col_name = self.extract_column_name(&first_order.expr)?;
        let col_idx = table_def
            .column_index(&col_name)
            .ok_or_else(|| BarqError::ColumnNotFound(col_name, table_def.name.clone()))?;

        let sort_options = SortOptions {
            descending: !first_order.asc.unwrap_or(true),
            nulls_first: first_order.nulls_first.unwrap_or(false),
        };

        let indices = sort_to_indices(batch.column(col_idx).as_ref(), Some(sort_options), None)?;

        let columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None).map(|a| a as ArrayRef))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(RecordBatch::try_new(batch.schema(), columns)?)
    }

    fn limit_batch(&self, batch: &RecordBatch, limit: &Expr) -> BarqResult<RecordBatch> {
        let n = match limit {
            Expr::Value(SqlValue::Number(n, _)) => {
                n.parse::<usize>().map_err(|_| BarqError::ParseError("Invalid LIMIT".to_string()))?
            }
            _ => return Err(BarqError::ParseError("Invalid LIMIT expression".to_string())),
        };

        if n >= batch.num_rows() {
            return Ok(batch.clone());
        }

        let indices: UInt64Array = (0..n as u64).collect();
        let columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None).map(|a| a as ArrayRef))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(RecordBatch::try_new(batch.schema(), columns)?)
    }

    fn project_columns(
        &self,
        batch: &RecordBatch,
        projection: &[SelectItem],
        table_def: &TableDef,
    ) -> BarqResult<(Arc<Schema>, RecordBatch)> {
        // Handle SELECT *
        if projection.len() == 1 {
            if let SelectItem::Wildcard(_) = &projection[0] {
                return Ok((batch.schema(), batch.clone()));
            }
        }

        let mut fields = Vec::new();
        let mut columns = Vec::new();

        for item in projection {
            match item {
                SelectItem::Wildcard(_) => {
                    for (i, field) in batch.schema().fields().iter().enumerate() {
                        fields.push(field.clone());
                        columns.push(batch.column(i).clone());
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    let col_name = self.extract_column_name(expr)?;
                    let col_idx = table_def.column_index(&col_name).ok_or_else(|| {
                        BarqError::ColumnNotFound(col_name.clone(), table_def.name.clone())
                    })?;
                    fields.push(batch.schema().field(col_idx).clone());
                    columns.push(batch.column(col_idx).clone());
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let col_name = self.extract_column_name(expr)?;
                    let col_idx = table_def.column_index(&col_name).ok_or_else(|| {
                        BarqError::ColumnNotFound(col_name.clone(), table_def.name.clone())
                    })?;
                    let field = batch.schema().field(col_idx);
                    fields.push(Arc::new(Field::new(
                        &alias.value,
                        field.data_type().clone(),
                        field.is_nullable(),
                    )));
                    columns.push(batch.column(col_idx).clone());
                }
                _ => {}
            }
        }

        let schema = Arc::new(Schema::new(fields));
        let result = RecordBatch::try_new(schema.clone(), columns)?;
        Ok((schema, result))
    }

    fn apply_join(
        &self,
        _left_batches: &[Arc<RecordBatch>],
        _table_def: &TableDef,
        _join: &sqlparser::ast::Join,
    ) -> BarqResult<Vec<Arc<RecordBatch>>> {
        Err(BarqError::UnsupportedFeature("JOINs not yet implemented".to_string()))
    }

    fn execute_update(
        &self,
        table: &TableWithJoins,
        _assignments: &[sqlparser::ast::Assignment],
        _selection: Option<&Expr>,
    ) -> BarqResult<QueryResult> {
        let _table_name = match &table.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err(BarqError::UnsupportedStatement("Invalid UPDATE".to_string())),
        };

        // TODO: Implement UPDATE
        Err(BarqError::UnsupportedFeature("UPDATE not yet implemented".to_string()))
    }

    fn execute_delete(
        &self,
        from: &[TableWithJoins],
        selection: Option<&Expr>,
    ) -> BarqResult<QueryResult> {
        let table_name = from
            .first()
            .and_then(|t| match &t.relation {
                TableFactor::Table { name, .. } => Some(name.to_string()),
                _ => None,
            })
            .ok_or_else(|| BarqError::ParseError("DELETE requires FROM".to_string()))?;

        let table = self.storage.get_table(&table_name)?;
        let table_def = table.def().clone();

        let deleted = if let Some(predicate) = selection {
            table.delete_where(|row_idx, batch| {
                let mask = self.eval_predicate(batch, predicate, &table_def).unwrap_or_else(|_| {
                    BooleanArray::from(vec![false; batch.num_rows()])
                });
                mask.value(row_idx)
            })?
        } else {
            let count = table.row_count();
            table.truncate();
            count
        };

        Ok(QueryResult::Command {
            tag: "DELETE".to_string(),
            rows_affected: deleted as u64,
        })
    }

    fn execute_truncate(&self, tables: &[sqlparser::ast::TruncateTableTarget]) -> BarqResult<QueryResult> {
        for target in tables {
            let name = target.name.to_string();
            let table = self.storage.get_table(&name)?;
            table.truncate();
        }

        Ok(QueryResult::Command {
            tag: "TRUNCATE TABLE".to_string(),
            rows_affected: 0,
        })
    }

    fn execute_alter_table(
        &self,
        _name: &ObjectName,
        _operations: &[sqlparser::ast::AlterTableOperation],
    ) -> BarqResult<QueryResult> {
        // TODO: Implement ALTER TABLE
        Ok(QueryResult::Command {
            tag: "ALTER TABLE".to_string(),
            rows_affected: 0,
        })
    }

    fn execute_create_index(
        &self,
        _create_index: &sqlparser::ast::CreateIndex,
    ) -> BarqResult<QueryResult> {
        // TODO: Implement CREATE INDEX
        Ok(QueryResult::Command {
            tag: "CREATE INDEX".to_string(),
            rows_affected: 0,
        })
    }

    fn execute_show_variable(&self, _stmt: &Statement) -> BarqResult<QueryResult> {
        // Return empty result for SHOW commands
        let schema = Arc::new(Schema::new(vec![Field::new("value", ArrowDataType::Utf8, true)]));
        Ok(QueryResult::Rows {
            schema,
            batches: vec![],
        })
    }

    fn execute_system_query(
        &self,
        _select: &Select,
        table_name: &str,
    ) -> BarqResult<QueryResult> {
        // Handle information_schema and pg_catalog queries
        if table_name.contains("tables") {
            return self.query_information_schema_tables();
        }
        if table_name.contains("columns") {
            return self.query_information_schema_columns();
        }

        // Return empty result for unhandled system tables
        let schema = Arc::new(Schema::empty());
        Ok(QueryResult::Rows {
            schema,
            batches: vec![],
        })
    }

    fn query_information_schema_tables(&self) -> BarqResult<QueryResult> {
        let tables = self.catalog.list_tables();

        let mut catalog_builder = StringBuilder::new();
        let mut schema_builder = StringBuilder::new();
        let mut name_builder = StringBuilder::new();
        let mut type_builder = StringBuilder::new();

        for table in tables {
            catalog_builder.append_value("barq");
            schema_builder.append_value("public");
            name_builder.append_value(&table);
            type_builder.append_value("BASE TABLE");
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", ArrowDataType::Utf8, true),
            Field::new("table_schema", ArrowDataType::Utf8, true),
            Field::new("table_name", ArrowDataType::Utf8, true),
            Field::new("table_type", ArrowDataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(catalog_builder.finish()),
                Arc::new(schema_builder.finish()),
                Arc::new(name_builder.finish()),
                Arc::new(type_builder.finish()),
            ],
        )?;

        Ok(QueryResult::Rows {
            schema,
            batches: vec![batch],
        })
    }

    fn query_information_schema_columns(&self) -> BarqResult<QueryResult> {
        let tables = self.catalog.list_tables();

        let mut catalog_builder = StringBuilder::new();
        let mut schema_builder = StringBuilder::new();
        let mut table_builder = StringBuilder::new();
        let mut column_builder = StringBuilder::new();
        let mut ordinal_builder = Int32Builder::new();
        let mut type_builder = StringBuilder::new();
        let mut nullable_builder = StringBuilder::new();

        for table_name in tables {
            if let Ok(table_def) = self.catalog.get_table(&table_name) {
                for (i, col) in table_def.columns.iter().enumerate() {
                    catalog_builder.append_value("barq");
                    schema_builder.append_value("public");
                    table_builder.append_value(&table_name);
                    column_builder.append_value(&col.name);
                    ordinal_builder.append_value((i + 1) as i32);
                    type_builder.append_value(format!("{:?}", col.data_type));
                    nullable_builder.append_value(if col.nullable { "YES" } else { "NO" });
                }
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", ArrowDataType::Utf8, true),
            Field::new("table_schema", ArrowDataType::Utf8, true),
            Field::new("table_name", ArrowDataType::Utf8, true),
            Field::new("column_name", ArrowDataType::Utf8, true),
            Field::new("ordinal_position", ArrowDataType::Int32, true),
            Field::new("data_type", ArrowDataType::Utf8, true),
            Field::new("is_nullable", ArrowDataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(catalog_builder.finish()),
                Arc::new(schema_builder.finish()),
                Arc::new(table_builder.finish()),
                Arc::new(column_builder.finish()),
                Arc::new(ordinal_builder.finish()),
                Arc::new(type_builder.finish()),
                Arc::new(nullable_builder.finish()),
            ],
        )?;

        Ok(QueryResult::Rows {
            schema,
            batches: vec![batch],
        })
    }

    fn extract_column_name(&self, expr: &Expr) -> BarqResult<String> {
        match expr {
            Expr::Identifier(ident) => Ok(ident.value.clone()),
            Expr::CompoundIdentifier(idents) => {
                Ok(idents.last().map(|i| i.value.clone()).unwrap_or_default())
            }
            _ => Err(BarqError::ParseError(format!(
                "Expected column name, got {:?}",
                expr
            ))),
        }
    }

    fn eval_expr_to_value(&self, expr: &Expr, target_type: &BarqType) -> BarqResult<Value> {
        match expr {
            Expr::Value(SqlValue::Null) => Ok(Value::Null),
            Expr::Value(SqlValue::Boolean(b)) => Ok(Value::Bool(*b)),
            Expr::Value(SqlValue::Number(n, _)) => {
                match target_type {
                    BarqType::Int16 => Ok(Value::Int16(
                        n.parse().map_err(|_| BarqError::InvalidValue {
                            column: "".to_string(),
                            value: n.clone(),
                        })?,
                    )),
                    BarqType::Int32 => Ok(Value::Int32(
                        n.parse().map_err(|_| BarqError::InvalidValue {
                            column: "".to_string(),
                            value: n.clone(),
                        })?,
                    )),
                    BarqType::Int64 => Ok(Value::Int64(
                        n.parse().map_err(|_| BarqError::InvalidValue {
                            column: "".to_string(),
                            value: n.clone(),
                        })?,
                    )),
                    BarqType::Float32 => Ok(Value::Float32(
                        n.parse().map_err(|_| BarqError::InvalidValue {
                            column: "".to_string(),
                            value: n.clone(),
                        })?,
                    )),
                    BarqType::Float64 => Ok(Value::Float64(
                        n.parse().map_err(|_| BarqError::InvalidValue {
                            column: "".to_string(),
                            value: n.clone(),
                        })?,
                    )),
                    _ => Ok(Value::Int64(
                        n.parse().map_err(|_| BarqError::InvalidValue {
                            column: "".to_string(),
                            value: n.clone(),
                        })?,
                    )),
                }
            }
            Expr::Value(SqlValue::SingleQuotedString(s)) | Expr::Value(SqlValue::DoubleQuotedString(s)) => {
                Ok(Value::Text(s.clone()))
            }
            Expr::UnaryOp { op: sqlparser::ast::UnaryOperator::Minus, expr } => {
                let inner = self.eval_expr_to_value(expr, target_type)?;
                match inner {
                    Value::Int16(n) => Ok(Value::Int16(-n)),
                    Value::Int32(n) => Ok(Value::Int32(-n)),
                    Value::Int64(n) => Ok(Value::Int64(-n)),
                    Value::Float32(n) => Ok(Value::Float32(-n)),
                    Value::Float64(n) => Ok(Value::Float64(-n)),
                    _ => Err(BarqError::TypeMismatch {
                        expected: "numeric".to_string(),
                        actual: format!("{:?}", inner),
                    }),
                }
            }
            _ => Err(BarqError::UnsupportedStatement(format!(
                "Unsupported expression: {:?}",
                expr
            ))),
        }
    }
}
