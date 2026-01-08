//! Struct defining a node-like entity in a knowledge graph.

use std::fmt::Display;

use sql_traits::traits::{DatabaseLike, TableLike};

use crate::primary_key::PrimaryKey;

#[derive(Debug, Clone)]
/// A struct representing a node-like entity in a knowledge graph.
pub struct Node<'db, DB: DatabaseLike> {
    /// The table from which the node originates.
    table: &'db DB::Table,
    /// The primary key values identifying the node.
    primary_key: PrimaryKey,
}

impl<DB: DatabaseLike> PartialEq for Node<'_, DB> {
    fn eq(&self, other: &Self) -> bool {
        self.table == other.table && self.primary_key == other.primary_key
    }
}

impl<DB: DatabaseLike> Eq for Node<'_, DB> {}

impl<DB: DatabaseLike> std::hash::Hash for Node<'_, DB> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.table.hash(state);
        self.primary_key.hash(state);
    }
}

impl<DB: DatabaseLike> PartialOrd for Node<'_, DB> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<DB: DatabaseLike> Ord for Node<'_, DB> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.table.cmp(other.table) {
            std::cmp::Ordering::Equal => self.primary_key.cmp(&other.primary_key),
            ord => ord,
        }
    }
}

impl<'db, DB: DatabaseLike> Node<'db, DB> {
    /// Create a new `Node` instance.
    ///
    /// # Arguments
    ///
    /// * `table` - The table from which the node originates.
    /// * `primary_key` - The primary key identifying the node.
    pub(crate) fn new(table: &'db DB::Table, primary_key: PrimaryKey) -> Self {
        Self { table, primary_key }
    }

    /// Returns a reference to the node's table.
    #[must_use]
    pub fn table(&self) -> &DB::Table {
        self.table
    }

    /// Returns the name of the node's table.
    #[must_use]
    pub fn table_name(&self) -> &str {
        self.table.table_name()
    }

    /// Returns the schema name of the node's table, if any.
    #[must_use]
    pub fn schema_name(&self) -> Option<&str> {
        self.table.table_schema()
    }
}

impl<DB: DatabaseLike> Display for Node<'_, DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(schema) = self.table.table_schema() {
            write!(f, "{schema}.")?;
        }
        write!(f, "{}({})", self.table.table_name(), self.primary_key)
    }
}
