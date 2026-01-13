use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::error::{BarqError, BarqResult};
use crate::types::BarqType;

/// Column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: BarqType,
    pub nullable: bool,
    pub default: Option<String>,
    pub is_primary_key: bool,
    pub is_unique: bool,
}

/// Table schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub schema: String, // PostgreSQL schema (default: public)
    pub columns: Vec<ColumnDef>,
    pub primary_key: Option<Vec<String>>, // Column names
    pub unique_constraints: Vec<Vec<String>>,
    pub created_at: i64,
}

impl TableDef {
    pub fn new(name: String) -> Self {
        Self {
            name,
            schema: "public".to_string(),
            columns: Vec::new(),
            primary_key: None,
            unique_constraints: Vec::new(),
            created_at: chrono::Utc::now().timestamp_micros(),
        }
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
    }

    pub fn get_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

/// Index definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
}

/// Database catalog - manages all schema information
pub struct Catalog {
    tables: RwLock<HashMap<String, TableDef>>,
    indexes: RwLock<HashMap<String, IndexDef>>,
    sequences: RwLock<HashMap<String, AtomicU64>>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            indexes: RwLock::new(HashMap::new()),
            sequences: RwLock::new(HashMap::new()),
        }
    }

    pub fn create_table(&self, def: TableDef) -> BarqResult<()> {
        let mut tables = self.tables.write();
        let full_name = format!("{}.{}", def.schema, def.name);

        if tables.contains_key(&full_name) || tables.contains_key(&def.name) {
            return Err(BarqError::TableAlreadyExists(def.name.clone()));
        }

        tables.insert(full_name.clone(), def.clone());
        tables.insert(def.name.clone(), def);
        Ok(())
    }

    pub fn drop_table(&self, name: &str) -> BarqResult<()> {
        let mut tables = self.tables.write();

        let full_name = format!("public.{}", name);
        if tables.remove(&full_name).is_none() && tables.remove(name).is_none() {
            return Err(BarqError::TableNotFound(name.to_string()));
        }

        // Also remove simple name if exists
        tables.remove(name);

        Ok(())
    }

    pub fn get_table(&self, name: &str) -> BarqResult<TableDef> {
        let tables = self.tables.read();

        // Try full name first
        let full_name = format!("public.{}", name);
        if let Some(def) = tables.get(&full_name) {
            return Ok(def.clone());
        }

        // Try simple name
        tables
            .get(name)
            .cloned()
            .ok_or_else(|| BarqError::TableNotFound(name.to_string()))
    }

    pub fn table_exists(&self, name: &str) -> bool {
        let tables = self.tables.read();
        let full_name = format!("public.{}", name);
        tables.contains_key(&full_name) || tables.contains_key(name)
    }

    pub fn list_tables(&self) -> Vec<String> {
        let tables = self.tables.read();
        let mut result: HashSet<String> = HashSet::new();
        for def in tables.values() {
            result.insert(def.name.clone());
        }
        result.into_iter().collect()
    }

    pub fn create_index(&self, def: IndexDef) -> BarqResult<()> {
        let mut indexes = self.indexes.write();

        if indexes.contains_key(&def.name) {
            return Err(BarqError::ConstraintViolation(format!(
                "Index '{}' already exists",
                def.name
            )));
        }

        indexes.insert(def.name.clone(), def);
        Ok(())
    }

    pub fn get_indexes_for_table(&self, table_name: &str) -> Vec<IndexDef> {
        let indexes = self.indexes.read();
        indexes
            .values()
            .filter(|idx| idx.table_name == table_name)
            .cloned()
            .collect()
    }

    pub fn create_sequence(&self, name: &str, start: u64) -> BarqResult<()> {
        let mut sequences = self.sequences.write();
        sequences.insert(name.to_string(), AtomicU64::new(start));
        Ok(())
    }

    pub fn next_sequence_value(&self, name: &str) -> BarqResult<u64> {
        let sequences = self.sequences.read();
        let seq = sequences
            .get(name)
            .ok_or_else(|| BarqError::Internal(format!("Sequence '{}' not found", name)))?;
        Ok(seq.fetch_add(1, Ordering::SeqCst))
    }

    pub fn alter_table_add_column(&self, table_name: &str, column: ColumnDef) -> BarqResult<()> {
        let mut tables = self.tables.write();

        let table = tables
            .get_mut(table_name)
            .ok_or_else(|| BarqError::TableNotFound(table_name.to_string()))?;

        if table.columns.iter().any(|c| c.name == column.name) {
            return Err(BarqError::ConstraintViolation(format!(
                "Column '{}' already exists in table '{}'",
                column.name, table_name
            )));
        }

        table.columns.push(column);
        Ok(())
    }

    pub fn alter_table_drop_column(&self, table_name: &str, column_name: &str) -> BarqResult<()> {
        let mut tables = self.tables.write();

        let table = tables
            .get_mut(table_name)
            .ok_or_else(|| BarqError::TableNotFound(table_name.to_string()))?;

        let idx = table
            .column_index(column_name)
            .ok_or_else(|| BarqError::ColumnNotFound(column_name.to_string(), table_name.to_string()))?;

        table.columns.remove(idx);
        Ok(())
    }
}

/// Schema for information_schema compatibility
pub struct InformationSchema;

impl InformationSchema {
    pub fn tables_columns() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "table_catalog".to_string(),
                data_type: BarqType::Text,
                nullable: true,
                default: None,
                is_primary_key: false,
                is_unique: false,
            },
            ColumnDef {
                name: "table_schema".to_string(),
                data_type: BarqType::Text,
                nullable: true,
                default: None,
                is_primary_key: false,
                is_unique: false,
            },
            ColumnDef {
                name: "table_name".to_string(),
                data_type: BarqType::Text,
                nullable: true,
                default: None,
                is_primary_key: false,
                is_unique: false,
            },
            ColumnDef {
                name: "table_type".to_string(),
                data_type: BarqType::Text,
                nullable: true,
                default: None,
                is_primary_key: false,
                is_unique: false,
            },
        ]
    }

    pub fn columns_columns() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "table_catalog".to_string(),
                data_type: BarqType::Text,
                nullable: true,
                default: None,
                is_primary_key: false,
                is_unique: false,
            },
            ColumnDef {
                name: "table_schema".to_string(),
                data_type: BarqType::Text,
                nullable: true,
                default: None,
                is_primary_key: false,
                is_unique: false,
            },
            ColumnDef {
                name: "table_name".to_string(),
                data_type: BarqType::Text,
                nullable: true,
                default: None,
                is_primary_key: false,
                is_unique: false,
            },
            ColumnDef {
                name: "column_name".to_string(),
                data_type: BarqType::Text,
                nullable: true,
                default: None,
                is_primary_key: false,
                is_unique: false,
            },
            ColumnDef {
                name: "ordinal_position".to_string(),
                data_type: BarqType::Int32,
                nullable: true,
                default: None,
                is_primary_key: false,
                is_unique: false,
            },
            ColumnDef {
                name: "data_type".to_string(),
                data_type: BarqType::Text,
                nullable: true,
                default: None,
                is_primary_key: false,
                is_unique: false,
            },
            ColumnDef {
                name: "is_nullable".to_string(),
                data_type: BarqType::Text,
                nullable: true,
                default: None,
                is_primary_key: false,
                is_unique: false,
            },
        ]
    }
}
