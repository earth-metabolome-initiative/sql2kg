//! Struct defining a node-like entity in a knowledge graph.

use std::fmt::Display;

use crate::primary_key::PrimaryKey;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// A struct representing a node-like entity in a knowledge graph.
pub struct Node<'db> {
    /// The name of the node's class schema.
    schema_name: Option<&'db str>,
    /// The name of the node's class (table name).
    table_name: &'db str,
    /// The primary key values identifying the node.
    primary_key: PrimaryKey,
}

impl<'db> Node<'db> {
    /// Create a new `Node` instance.
    ///
    /// # Arguments
    ///
    /// * `schema_name` - The name of the node's class schema.
    /// * `table_name` - The name of the node's class (table name).
    /// * `primary_key` - The primary key identifying the node.
    pub(crate) fn new(
        schema_name: Option<&'db str>,
        table_name: &'db str,
        primary_key: PrimaryKey,
    ) -> Self {
        Self { schema_name, table_name, primary_key }
    }

    /// Returns a reference to the node's table schema name, if any.
    #[must_use]
    pub fn schema_name(&self) -> Option<&str> {
        self.schema_name
    }

    /// Returns a reference to the node's table name.
    #[must_use]
    pub fn table_name(&self) -> &str {
        self.table_name
    }

    /// Returns the node class name in "schema.table" format if schema exists,
    /// otherwise just "table".
    #[must_use]
    pub fn class_name(&self) -> String {
        if let Some(schema) = self.schema_name {
            format!("{}.{}", schema, self.table_name)
        } else {
            self.table_name.to_string()
        }
    }
}

impl Display for Node<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(schema) = self.schema_name {
            write!(f, "{}.{}({})", schema, self.table_name, self.primary_key)
        } else {
            write!(f, "{}({})", self.table_name, self.primary_key)
        }
    }
}
