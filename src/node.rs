//! Struct defining a node-like entity in a knowledge graph.

use std::fmt::Display;

use crate::primary_key::PrimaryKey;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// A struct representing a node-like entity in a knowledge graph.
pub struct Node<'db> {
    /// The name of the node's class (table name).
    class_name: &'db str,
    /// The primary key values identifying the node.
    primary_key: PrimaryKey,
}

impl<'db> Node<'db> {
    /// Create a new `Node` instance.
    ///
    /// # Arguments
    ///
    /// * `class_name` - The name of the node's class (table name).
    /// * `primary_key` - The primary key identifying the node.
    pub(crate) fn new(class_name: &'db str, primary_key: PrimaryKey) -> Self {
        Self { class_name, primary_key }
    }

    /// Returns a reference to the node's class name.
    #[must_use]
    pub fn class_name(&self) -> &str {
        self.class_name
    }
}

impl Display for Node<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.class_name, self.primary_key)
    }
}
