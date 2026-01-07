//! Struct defining an edge class in a knowledge graph.

use std::fmt::Display;

/// A struct representing an edge class in a knowledge graph.
pub struct EdgeClass<'db> {
    /// The name of the host node's class (table name).
    host_class_name: &'db str,
    /// The name of the referenced node's class (table name).
    referenced_class_name: &'db str,
    /// The foreign key column names in the host table.
    host_column_names: Vec<&'db str>,
    /// The foreign key column names in the referenced table.
    referenced_column_names: Vec<&'db str>,
}

impl<'db> EdgeClass<'db> {
    /// Create a new `EdgeClass` instance.
    ///
    /// # Arguments
    ///
    /// * `host_class_name` - The name of the host node's class (table name).
    /// * `referenced_class_name` - The name of the referenced node's class
    ///   (table name).
    /// * `host_column_names` - The foreign key column names in the host table.
    /// * `referenced_column_names` - The foreign key column names in the
    ///   referenced table.
    pub(crate) fn new(
        host_class_name: &'db str,
        referenced_class_name: &'db str,
        host_column_names: Vec<&'db str>,
        referenced_column_names: Vec<&'db str>,
    ) -> Self {
        Self { host_class_name, referenced_class_name, host_column_names, referenced_column_names }
    }
}

impl Display for EdgeClass<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let host_cols = self.host_column_names.join(", ");
        let ref_cols = self.referenced_column_names.join(", ");
        write!(
            f,
            "{}({}) -> {}({})",
            self.host_class_name, host_cols, self.referenced_class_name, ref_cols
        )
    }
}
