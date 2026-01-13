use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Builder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};

use crate::error::{DbError, DbResult};

#[derive(Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<(String, DataType)>,
}

impl TableSchema {
    pub fn to_arrow_schema(&self) -> Schema {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|(name, dtype)| Field::new(name, dtype.clone(), true))
            .collect();
        Schema::new(fields)
    }
}

pub struct ColumnarTable {
    schema: TableSchema,
    batches: Vec<Arc<RecordBatch>>,
}

impl ColumnarTable {
    pub fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            batches: Vec::new(),
        }
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    pub fn scan(&self) -> Vec<Arc<RecordBatch>> {
        self.batches.clone()
    }

    pub fn insert_batch(&mut self, batch: RecordBatch) -> DbResult<()> {
        self.batches.push(Arc::new(batch));
        Ok(())
    }
}

pub struct Catalog {
    tables: HashMap<String, ColumnarTable>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, schema: TableSchema) -> DbResult<()> {
        if self.tables.contains_key(&schema.name) {
            return Err(DbError::TableAlreadyExists(schema.name.clone()));
        }
        let name = schema.name.clone();
        self.tables.insert(name, ColumnarTable::new(schema));
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> DbResult<&ColumnarTable> {
        self.tables
            .get(name)
            .ok_or_else(|| DbError::TableNotFound(name.to_string()))
    }

    pub fn get_table_mut(&mut self, name: &str) -> DbResult<&mut ColumnarTable> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| DbError::TableNotFound(name.to_string()))
    }
}

pub struct BatchBuilder {
    schema: Arc<Schema>,
    int_builders: HashMap<String, Int32Builder>,
    string_builders: HashMap<String, StringBuilder>,
}

impl BatchBuilder {
    pub fn new(table_schema: &TableSchema) -> Self {
        let arrow_schema = Arc::new(table_schema.to_arrow_schema());
        let mut int_builders = HashMap::new();
        let mut string_builders = HashMap::new();

        for (name, dtype) in &table_schema.columns {
            match dtype {
                DataType::Int32 => {
                    int_builders.insert(name.clone(), Int32Builder::new());
                }
                DataType::Utf8 => {
                    string_builders.insert(name.clone(), StringBuilder::new());
                }
                _ => {}
            }
        }

        Self {
            schema: arrow_schema,
            int_builders,
            string_builders,
        }
    }

    pub fn append_int(&mut self, column: &str, value: i32) -> DbResult<()> {
        self.int_builders
            .get_mut(column)
            .ok_or_else(|| DbError::ColumnNotFound(column.to_string(), "unknown".to_string()))?
            .append_value(value);
        Ok(())
    }

    pub fn append_string(&mut self, column: &str, value: &str) -> DbResult<()> {
        self.string_builders
            .get_mut(column)
            .ok_or_else(|| DbError::ColumnNotFound(column.to_string(), "unknown".to_string()))?
            .append_value(value);
        Ok(())
    }

    pub fn finish(mut self) -> DbResult<RecordBatch> {
        let mut columns: Vec<ArrayRef> = Vec::new();

        for field in self.schema.fields() {
            let array: ArrayRef = match field.data_type() {
                DataType::Int32 => {
                    let builder = self.int_builders.get_mut(field.name()).ok_or_else(|| {
                        DbError::ColumnNotFound(field.name().to_string(), "batch".to_string())
                    })?;
                    Arc::new(builder.finish())
                }
                DataType::Utf8 => {
                    let builder = self.string_builders.get_mut(field.name()).ok_or_else(|| {
                        DbError::ColumnNotFound(field.name().to_string(), "batch".to_string())
                    })?;
                    Arc::new(builder.finish())
                }
                _ => {
                    return Err(DbError::UnsupportedStatement(format!(
                        "Unsupported data type: {:?}",
                        field.data_type()
                    )))
                }
            };
            columns.push(array);
        }

        RecordBatch::try_new(self.schema, columns).map_err(DbError::from)
    }
}
