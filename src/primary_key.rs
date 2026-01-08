//! Submodule defining what are valid primary key-like constructs in a
//! knowledge graph-like database.

use std::{fmt::Display, num::NonZeroU32};

use diesel::{
    deserialize::{self, FromSql},
    pg::{Pg, PgValue},
};
use diesel_dynamic_schema::dynamic_value::Any;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

const VARCHAR_OID: NonZeroU32 = NonZeroU32::new(1043).expect("OID must be non-zero");
const TEXT_OID: NonZeroU32 = NonZeroU32::new(25).expect("OID must be non-zero");
const INTEGER_OID: NonZeroU32 = NonZeroU32::new(23).expect("OID must be non-zero");
const BIGINT_OID: NonZeroU32 = NonZeroU32::new(20).expect("OID must be non-zero");
const UUID_OID: NonZeroU32 = NonZeroU32::new(2950).expect("OID must be non-zero");

impl FromSql<Any, Pg> for PrimaryKey {
    fn from_sql(value: PgValue) -> deserialize::Result<Self> {
        match value.get_oid() {
            VARCHAR_OID | TEXT_OID => {
                <String as FromSql<diesel::sql_types::Text, Pg>>::from_sql(value)
                    .map(PrimaryKey::String)
            }
            INTEGER_OID => {
                <i32 as FromSql<diesel::sql_types::Integer, Pg>>::from_sql(value)
                    .map(PrimaryKey::I32)
            }
            BIGINT_OID => {
                <i64 as FromSql<diesel::sql_types::BigInt, Pg>>::from_sql(value)
                    .map(PrimaryKey::I64)
            }
            UUID_OID => {
                <uuid::Uuid as FromSql<diesel::sql_types::Uuid, Pg>>::from_sql(value)
                    .map(PrimaryKey::UUID)
            }
            e => Err(format!("Unknown type: {e}").into()),
        }
    }
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

impl From<Vec<PrimaryKey>> for PrimaryKey {
    fn from(v: Vec<PrimaryKey>) -> Self {
        if v.len() == 1 {
            return v.into_iter().next().expect("Vector has one element");
        }
        PrimaryKey::Composite(v)
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
