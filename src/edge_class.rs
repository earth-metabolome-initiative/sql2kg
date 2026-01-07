//! Struct defining an edge class in a knowledge graph.

use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// A struct representing an edge class in a knowledge graph.
pub struct EdgeClass<'db> {
    /// The schema name of the referenced node's class (table name).
    table_schema: Option<&'db str>,
    /// The name of the host node's class (table name).
    table_name: &'db str,
    /// The foreign key column names in the host table.
    column_names: Vec<&'db str>,
}

impl<'db> EdgeClass<'db> {
    /// Create a new `EdgeClass` instance.
    ///
    /// # Arguments
    ///
    /// * `schema_name` - The schema name of the referenced node's class (table
    ///   name).
    /// * `table_name` - The name of the host node's class (table name).
    /// * `column_names` - The foreign key column names in the host table.
    pub(crate) fn new(
        table_schema: Option<&'db str>,
        table_name: &'db str,
        column_names: Vec<&'db str>,
    ) -> Self {
        Self { table_schema, table_name, column_names }
    }
}

impl Display for EdgeClass<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(schema) = self.table_schema {
            write!(f, "{}.{}({})", schema, self.table_name, self.column_names.join(", "))
        } else {
            write!(f, "{}({})", self.table_name, self.column_names.join(", "))
        }
    }
}
