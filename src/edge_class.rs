//! Struct defining an edge class in a knowledge graph.

use std::fmt::Display;

use sql_traits::traits::{ColumnLike, DatabaseLike, TableLike};

#[derive(Debug, Clone)]
/// A struct representing an edge class in a knowledge graph.
pub struct EdgeClass<'db, DB: DatabaseLike> {
    /// The table from which the edge originates.
    host_table: &'db DB::Table,
    /// The column names representing the foreign key in the host table.
    columns: Vec<&'db DB::Column>,
}

impl<DB: DatabaseLike> PartialEq for EdgeClass<'_, DB> {
    fn eq(&self, other: &Self) -> bool {
        self.host_table == other.host_table && self.columns == other.columns
    }
}

impl<DB: DatabaseLike> Eq for EdgeClass<'_, DB> {}

impl<DB: DatabaseLike> PartialOrd for EdgeClass<'_, DB> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<DB: DatabaseLike> Ord for EdgeClass<'_, DB> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.host_table.cmp(other.host_table) {
            std::cmp::Ordering::Equal => self.columns.cmp(&other.columns),
            ord => ord,
        }
    }
}

impl<DB: DatabaseLike> std::hash::Hash for EdgeClass<'_, DB> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.host_table.hash(state);
        for column in &self.columns {
            column.hash(state);
        }
    }
}

impl<'db, DB: DatabaseLike> EdgeClass<'db, DB> {
    /// Create a new `EdgeClass` instance.
    ///
    /// # Arguments
    ///
    /// * `host_table` - The table from which the edge originates.
    /// * `columns` - The columns representing the foreign key in the host
    ///   table.
    pub(crate) fn new(host_table: &'db DB::Table, columns: Vec<&'db DB::Column>) -> Self {
        Self { host_table, columns }
    }
}

impl<DB: DatabaseLike> Display for EdgeClass<'_, DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(schema) = self.host_table.table_schema() {
            write!(f, "{schema}.")?;
        }
        write!(f, "{}(", self.host_table.table_name())?;
        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", column.column_name())?;
        }
        write!(f, ")")
    }
}
