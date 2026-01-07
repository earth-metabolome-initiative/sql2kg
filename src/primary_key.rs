//! Submodule defining what are valid primary key-like constructs in a
//! knowledge graph-like database.

use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// An enum representing valid primary key-like constructs in a knowledge
/// graph-like database.
pub enum PrimaryKey {
    /// A string primary key value.
    String(String),
    /// An integer (32-bit) primary key value.
    I32(i32),
    /// An integer (64-bit) primary key value.
    I64(i64),
    /// A UUID primary key value.
    UUID(uuid::Uuid),
    /// A composite primary key value.
    Composite(Vec<PrimaryKey>),
}

impl From<String> for PrimaryKey {
    fn from(s: String) -> Self {
        PrimaryKey::String(s)
    }
}

impl From<i32> for PrimaryKey {
    fn from(i: i32) -> Self {
        PrimaryKey::I32(i)
    }
}

impl From<i64> for PrimaryKey {
    fn from(i: i64) -> Self {
        PrimaryKey::I64(i)
    }
}

impl From<uuid::Uuid> for PrimaryKey {
    fn from(u: uuid::Uuid) -> Self {
        PrimaryKey::UUID(u)
    }
}

impl Display for PrimaryKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrimaryKey::String(s) => write!(f, "{s}"),
            PrimaryKey::I32(i) => write!(f, "{i}"),
            PrimaryKey::I64(i) => write!(f, "{i}"),
            PrimaryKey::UUID(u) => write!(f, "{u}"),
            PrimaryKey::Composite(pk_vec) => {
                let pk_strings: Vec<String> = pk_vec.iter().map(|pk| format!("{pk}")).collect();
                write!(f, "{}", pk_strings.join(", "))
            }
        }
    }
}
