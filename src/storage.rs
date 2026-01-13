use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;

use crate::catalog::TableDef;
use crate::error::{BarqError, BarqResult};
use crate::types::{BarqType, Value};

/// In-memory table storage using Arrow columnar format
pub struct Table {
    def: TableDef,
    batches: RwLock<Vec<Arc<RecordBatch>>>,
    indexes: RwLock<HashMap<String, BTreeIndex>>,
    row_count: RwLock<usize>,
}

impl Table {
    pub fn new(def: TableDef) -> Self {
        Self {
            def,
            batches: RwLock::new(Vec::new()),
            indexes: RwLock::new(HashMap::new()),
            row_count: RwLock::new(0),
        }
    }

    pub fn def(&self) -> &TableDef {
        &self.def
    }

    pub fn schema(&self) -> Arc<Schema> {
        let fields: Vec<Field> = self
            .def
            .columns
            .iter()
            .map(|col| Field::new(&col.name, col.data_type.to_arrow(), col.nullable))
            .collect();
        Arc::new(Schema::new(fields))
    }

    pub fn insert(&self, batch: RecordBatch) -> BarqResult<usize> {
        let row_count = batch.num_rows();

        // Check constraints
        self.check_constraints(&batch)?;

        // Update indexes
        self.update_indexes(&batch)?;

        // Store batch
        let mut batches = self.batches.write();
        batches.push(Arc::new(batch));

        let mut count = self.row_count.write();
        *count += row_count;

        Ok(row_count)
    }

    fn check_constraints(&self, batch: &RecordBatch) -> BarqResult<()> {
        for (col_idx, col_def) in self.def.columns.iter().enumerate() {
            let array = batch.column(col_idx);

            // Check NOT NULL constraint
            if !col_def.nullable && array.null_count() > 0 {
                return Err(BarqError::NotNullViolation(col_def.name.clone()));
            }
        }
        Ok(())
    }

    fn update_indexes(&self, batch: &RecordBatch) -> BarqResult<()> {
        let mut indexes = self.indexes.write();
        let batches = self.batches.read();
        let batch_idx = batches.len();

        for (idx_name, index) in indexes.iter_mut() {
            let col_idx = self
                .def
                .column_index(&index.column_name)
                .ok_or_else(|| BarqError::Internal(format!("Index column not found: {}", index.column_name)))?;

            let array = batch.column(col_idx);
            index.insert_batch(batch_idx, array, index.is_unique)?;
        }

        Ok(())
    }

    pub fn create_index(&self, name: &str, column: &str, is_unique: bool) -> BarqResult<()> {
        let col_idx = self
            .def
            .column_index(column)
            .ok_or_else(|| BarqError::ColumnNotFound(column.to_string(), self.def.name.clone()))?;

        let mut index = BTreeIndex::new(column.to_string(), is_unique);

        // Index existing data
        let batches = self.batches.read();
        for (batch_idx, batch) in batches.iter().enumerate() {
            let array = batch.column(col_idx);
            index.insert_batch(batch_idx, array, is_unique)?;
        }

        let mut indexes = self.indexes.write();
        indexes.insert(name.to_string(), index);

        Ok(())
    }

    pub fn scan(&self) -> Vec<Arc<RecordBatch>> {
        self.batches.read().clone()
    }

    pub fn scan_with_filter<F>(&self, filter: F) -> BarqResult<Vec<RecordBatch>>
    where
        F: Fn(&RecordBatch) -> BarqResult<RecordBatch>,
    {
        let batches = self.batches.read();
        let mut results = Vec::new();

        for batch in batches.iter() {
            let filtered = filter(batch)?;
            if filtered.num_rows() > 0 {
                results.push(filtered);
            }
        }

        Ok(results)
    }

    pub fn row_count(&self) -> usize {
        *self.row_count.read()
    }

    pub fn truncate(&self) {
        let mut batches = self.batches.write();
        let mut indexes = self.indexes.write();
        let mut count = self.row_count.write();

        batches.clear();
        for index in indexes.values_mut() {
            index.clear();
        }
        *count = 0;
    }

    pub fn delete_where<F>(&self, predicate: F) -> BarqResult<usize>
    where
        F: Fn(usize, &RecordBatch) -> bool,
    {
        let mut batches = self.batches.write();
        let mut deleted = 0;
        let mut new_batches = Vec::new();

        for batch in batches.iter() {
            let mut keep_indices = Vec::new();
            for row_idx in 0..batch.num_rows() {
                if !predicate(row_idx, batch) {
                    keep_indices.push(row_idx as u64);
                } else {
                    deleted += 1;
                }
            }

            if !keep_indices.is_empty() {
                let indices = UInt64Array::from(keep_indices);
                let new_batch = take_record_batch(batch, &indices)?;
                new_batches.push(Arc::new(new_batch));
            }
        }

        *batches = new_batches;

        let mut count = self.row_count.write();
        *count = count.saturating_sub(deleted);

        Ok(deleted)
    }
}

/// B-tree index for fast lookups
pub struct BTreeIndex {
    column_name: String,
    is_unique: bool,
    // Maps value -> (batch_idx, row_idx within batch)
    entries: BTreeMap<IndexKey, Vec<(usize, usize)>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexKey {
    Null,
    Int(i64),
    Text(String),
    Bytes(Vec<u8>),
}

impl BTreeIndex {
    pub fn new(column_name: String, is_unique: bool) -> Self {
        Self {
            column_name,
            is_unique,
            entries: BTreeMap::new(),
        }
    }

    pub fn insert_batch(&mut self, batch_idx: usize, array: &ArrayRef, check_unique: bool) -> BarqResult<()> {
        for row_idx in 0..array.len() {
            let key = self.array_value_to_key(array, row_idx);

            if check_unique && self.is_unique {
                if let Some(existing) = self.entries.get(&key) {
                    if !existing.is_empty() && key != IndexKey::Null {
                        return Err(BarqError::UniqueViolation(self.column_name.clone()));
                    }
                }
            }

            self.entries
                .entry(key)
                .or_insert_with(Vec::new)
                .push((batch_idx, row_idx));
        }
        Ok(())
    }

    fn array_value_to_key(&self, array: &ArrayRef, idx: usize) -> IndexKey {
        if array.is_null(idx) {
            return IndexKey::Null;
        }

        match array.data_type() {
            ArrowDataType::Int16 => {
                let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
                IndexKey::Int(arr.value(idx) as i64)
            }
            ArrowDataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                IndexKey::Int(arr.value(idx) as i64)
            }
            ArrowDataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                IndexKey::Int(arr.value(idx))
            }
            ArrowDataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                IndexKey::Text(arr.value(idx).to_string())
            }
            ArrowDataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                IndexKey::Text(arr.value(idx).to_string())
            }
            _ => IndexKey::Null,
        }
    }

    pub fn lookup(&self, key: &IndexKey) -> Option<&Vec<(usize, usize)>> {
        self.entries.get(key)
    }

    pub fn range_scan(&self, start: &IndexKey, end: &IndexKey) -> Vec<(usize, usize)> {
        self.entries
            .range(start..=end)
            .flat_map(|(_, positions)| positions.iter().cloned())
            .collect()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

/// Storage engine managing all tables
pub struct StorageEngine {
    tables: RwLock<HashMap<String, Arc<Table>>>,
}

impl Default for StorageEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageEngine {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }

    pub fn create_table(&self, def: TableDef) -> BarqResult<()> {
        let mut tables = self.tables.write();
        let name = def.name.clone();

        if tables.contains_key(&name) {
            return Err(BarqError::TableAlreadyExists(name));
        }

        let table = Arc::new(Table::new(def));
        tables.insert(name, table);
        Ok(())
    }

    pub fn drop_table(&self, name: &str) -> BarqResult<()> {
        let mut tables = self.tables.write();
        tables
            .remove(name)
            .ok_or_else(|| BarqError::TableNotFound(name.to_string()))?;
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> BarqResult<Arc<Table>> {
        let tables = self.tables.read();
        tables
            .get(name)
            .cloned()
            .ok_or_else(|| BarqError::TableNotFound(name.to_string()))
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.read().contains_key(name)
    }
}

/// Helper function to take rows from a RecordBatch
fn take_record_batch(batch: &RecordBatch, indices: &UInt64Array) -> BarqResult<RecordBatch> {
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| arrow_select::take::take(col.as_ref(), indices, None).map(|a| a as ArrayRef))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// Builder for creating RecordBatches from row data
pub struct BatchBuilder {
    schema: Arc<Schema>,
    columns: Vec<Box<dyn ArrayBuilder>>,
}

impl BatchBuilder {
    pub fn new(table_def: &TableDef) -> Self {
        let schema = Arc::new(Schema::new(
            table_def
                .columns
                .iter()
                .map(|c| Field::new(&c.name, c.data_type.to_arrow(), c.nullable))
                .collect::<Vec<_>>(),
        ));

        let columns: Vec<Box<dyn ArrayBuilder>> = table_def
            .columns
            .iter()
            .map(|col| create_builder(&col.data_type))
            .collect();

        Self { schema, columns }
    }

    pub fn append_row(&mut self, values: &[Value]) -> BarqResult<()> {
        if values.len() != self.columns.len() {
            return Err(BarqError::Internal(format!(
                "Expected {} values, got {}",
                self.columns.len(),
                values.len()
            )));
        }

        for (i, value) in values.iter().enumerate() {
            append_value(&mut self.columns[i], value)?;
        }

        Ok(())
    }

    pub fn finish(self) -> BarqResult<RecordBatch> {
        let arrays: Vec<ArrayRef> = self
            .columns
            .into_iter()
            .map(|mut builder| builder.finish())
            .collect();

        Ok(RecordBatch::try_new(self.schema, arrays)?)
    }
}

fn create_builder(data_type: &BarqType) -> Box<dyn ArrayBuilder> {
    match data_type {
        BarqType::Bool => Box::new(BooleanBuilder::new()),
        BarqType::Int16 => Box::new(Int16Builder::new()),
        BarqType::Int32 => Box::new(Int32Builder::new()),
        BarqType::Int64 => Box::new(Int64Builder::new()),
        BarqType::Float32 => Box::new(Float32Builder::new()),
        BarqType::Float64 => Box::new(Float64Builder::new()),
        BarqType::Text | BarqType::Varchar(_) | BarqType::Uuid | BarqType::Json | BarqType::Jsonb => {
            Box::new(StringBuilder::new())
        }
        BarqType::Timestamp => Box::new(TimestampMicrosecondBuilder::new()),
        BarqType::Date => Box::new(Date32Builder::new()),
        BarqType::Bytea => Box::new(BinaryBuilder::new()),
        BarqType::Null => Box::new(NullBuilder::new()),
    }
}

fn append_value(builder: &mut Box<dyn ArrayBuilder>, value: &Value) -> BarqResult<()> {
    match value {
        Value::Null => {
            // Append null to any builder type
            match builder.as_any_mut().type_id() {
                _ if builder.as_any_mut().downcast_mut::<BooleanBuilder>().is_some() => {
                    builder.as_any_mut().downcast_mut::<BooleanBuilder>().unwrap().append_null();
                }
                _ if builder.as_any_mut().downcast_mut::<Int16Builder>().is_some() => {
                    builder.as_any_mut().downcast_mut::<Int16Builder>().unwrap().append_null();
                }
                _ if builder.as_any_mut().downcast_mut::<Int32Builder>().is_some() => {
                    builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap().append_null();
                }
                _ if builder.as_any_mut().downcast_mut::<Int64Builder>().is_some() => {
                    builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap().append_null();
                }
                _ if builder.as_any_mut().downcast_mut::<Float32Builder>().is_some() => {
                    builder.as_any_mut().downcast_mut::<Float32Builder>().unwrap().append_null();
                }
                _ if builder.as_any_mut().downcast_mut::<Float64Builder>().is_some() => {
                    builder.as_any_mut().downcast_mut::<Float64Builder>().unwrap().append_null();
                }
                _ if builder.as_any_mut().downcast_mut::<StringBuilder>().is_some() => {
                    builder.as_any_mut().downcast_mut::<StringBuilder>().unwrap().append_null();
                }
                _ if builder.as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>().is_some() => {
                    builder.as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>().unwrap().append_null();
                }
                _ if builder.as_any_mut().downcast_mut::<Date32Builder>().is_some() => {
                    builder.as_any_mut().downcast_mut::<Date32Builder>().unwrap().append_null();
                }
                _ if builder.as_any_mut().downcast_mut::<BinaryBuilder>().is_some() => {
                    builder.as_any_mut().downcast_mut::<BinaryBuilder>().unwrap().append_null();
                }
                _ => {}
            }
        }
        Value::Bool(v) => {
            builder.as_any_mut().downcast_mut::<BooleanBuilder>().unwrap().append_value(*v);
        }
        Value::Int16(v) => {
            builder.as_any_mut().downcast_mut::<Int16Builder>().unwrap().append_value(*v);
        }
        Value::Int32(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<Int32Builder>() {
                b.append_value(*v);
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
                b.append_value(*v as i64);
            }
        }
        Value::Int64(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
                b.append_value(*v);
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int32Builder>() {
                b.append_value(*v as i32);
            }
        }
        Value::Float32(v) => {
            builder.as_any_mut().downcast_mut::<Float32Builder>().unwrap().append_value(*v);
        }
        Value::Float64(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
                b.append_value(*v);
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<Float32Builder>() {
                b.append_value(*v as f32);
            }
        }
        Value::Text(v) => {
            builder.as_any_mut().downcast_mut::<StringBuilder>().unwrap().append_value(v);
        }
        Value::Timestamp(v) => {
            builder.as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>().unwrap().append_value(*v);
        }
        Value::Date(v) => {
            builder.as_any_mut().downcast_mut::<Date32Builder>().unwrap().append_value(*v);
        }
        Value::Bytea(v) => {
            builder.as_any_mut().downcast_mut::<BinaryBuilder>().unwrap().append_value(v);
        }
        Value::Uuid(v) => {
            builder.as_any_mut().downcast_mut::<StringBuilder>().unwrap().append_value(v);
        }
        Value::Json(v) => {
            builder.as_any_mut().downcast_mut::<StringBuilder>().unwrap().append_value(v);
        }
    }
    Ok(())
}

/// Trait for array builders
pub trait ArrayBuilder: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
    fn finish(&mut self) -> ArrayRef;
}

impl ArrayBuilder for BooleanBuilder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
    fn finish(&mut self) -> ArrayRef { Arc::new(BooleanBuilder::finish(self)) }
}

impl ArrayBuilder for Int16Builder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
    fn finish(&mut self) -> ArrayRef { Arc::new(Int16Builder::finish(self)) }
}

impl ArrayBuilder for Int32Builder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
    fn finish(&mut self) -> ArrayRef { Arc::new(Int32Builder::finish(self)) }
}

impl ArrayBuilder for Int64Builder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
    fn finish(&mut self) -> ArrayRef { Arc::new(Int64Builder::finish(self)) }
}

impl ArrayBuilder for Float32Builder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
    fn finish(&mut self) -> ArrayRef { Arc::new(Float32Builder::finish(self)) }
}

impl ArrayBuilder for Float64Builder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
    fn finish(&mut self) -> ArrayRef { Arc::new(Float64Builder::finish(self)) }
}

impl ArrayBuilder for StringBuilder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
    fn finish(&mut self) -> ArrayRef { Arc::new(StringBuilder::finish(self)) }
}

impl ArrayBuilder for TimestampMicrosecondBuilder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
    fn finish(&mut self) -> ArrayRef { Arc::new(TimestampMicrosecondBuilder::finish(self)) }
}

impl ArrayBuilder for Date32Builder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
    fn finish(&mut self) -> ArrayRef { Arc::new(Date32Builder::finish(self)) }
}

impl ArrayBuilder for BinaryBuilder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
    fn finish(&mut self) -> ArrayRef { Arc::new(BinaryBuilder::finish(self)) }
}

impl ArrayBuilder for NullBuilder {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
    fn finish(&mut self) -> ArrayRef { Arc::new(NullBuilder::finish(self)) }
}
