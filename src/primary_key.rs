//! Submodule defining what are valid primary key-like constructs in a
//! knowledge graph-like database.

use std::fmt::Display;

#[cfg(any(feature = "postgres", feature = "sqlite", feature = "mysql"))]
use diesel::deserialize::{self, FromSql};
#[cfg(any(feature = "postgres", feature = "sqlite", feature = "mysql"))]
use diesel_dynamic_schema::dynamic_value::Any;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// An enum representing valid primary key-like constructs in a knowledge
/// graph-like database.
pub enum PrimaryKey {
    /// A string primary key value.
    String(String),
    /// An integer primary key value.
    ///
    /// Every integer width decodes into this variant, so that a foreign key
    /// value compares equal to the primary key it references even when the
    /// two columns use different integer widths.
    Integer(i64),
    /// A UUID primary key value.
    UUID(uuid::Uuid),
    /// A composite primary key value.
    Composite(Vec<PrimaryKey>),
}

#[cfg(feature = "postgres")]
impl FromSql<Any, diesel::pg::Pg> for PrimaryKey {
    fn from_sql(value: diesel::pg::PgValue<'_>) -> deserialize::Result<Self> {
        use std::num::NonZeroU32;

        use diesel::{pg::Pg, sql_types};

        const VARCHAR_OID: NonZeroU32 = NonZeroU32::new(1043).expect("OID must be non-zero");
        const TEXT_OID: NonZeroU32 = NonZeroU32::new(25).expect("OID must be non-zero");
        const SMALLINT_OID: NonZeroU32 = NonZeroU32::new(21).expect("OID must be non-zero");
        const INTEGER_OID: NonZeroU32 = NonZeroU32::new(23).expect("OID must be non-zero");
        const BIGINT_OID: NonZeroU32 = NonZeroU32::new(20).expect("OID must be non-zero");
        const UUID_OID: NonZeroU32 = NonZeroU32::new(2950).expect("OID must be non-zero");

        match value.get_oid() {
            VARCHAR_OID | TEXT_OID => {
                <String as FromSql<sql_types::Text, Pg>>::from_sql(value).map(PrimaryKey::String)
            }
            SMALLINT_OID => {
                <i16 as FromSql<sql_types::SmallInt, Pg>>::from_sql(value).map(PrimaryKey::from)
            }
            INTEGER_OID => {
                <i32 as FromSql<sql_types::Integer, Pg>>::from_sql(value).map(PrimaryKey::from)
            }
            BIGINT_OID => {
                <i64 as FromSql<sql_types::BigInt, Pg>>::from_sql(value).map(PrimaryKey::from)
            }
            UUID_OID => {
                <uuid::Uuid as FromSql<sql_types::Uuid, Pg>>::from_sql(value).map(PrimaryKey::UUID)
            }
            e => Err(format!("Unknown type: {e}").into()),
        }
    }
}

#[cfg(feature = "sqlite")]
impl FromSql<Any, diesel::sqlite::Sqlite> for PrimaryKey {
    fn from_sql(value: diesel::sqlite::SqliteValue<'_, '_, '_>) -> deserialize::Result<Self> {
        use diesel::{
            sql_types,
            sqlite::{Sqlite, SqliteType},
        };

        // SQLite types values, not columns: every integer is stored as a
        // 64-bit `Long`, whatever width the column declares.
        match value.value_type() {
            Some(SqliteType::Text) => {
                <String as FromSql<sql_types::Text, Sqlite>>::from_sql(value)
                    .map(PrimaryKey::String)
            }
            Some(SqliteType::Long) => {
                <i64 as FromSql<sql_types::BigInt, Sqlite>>::from_sql(value).map(PrimaryKey::from)
            }
            e => Err(format!("Unknown type: {e:?}").into()),
        }
    }
}

#[cfg(feature = "mysql")]
impl FromSql<Any, diesel::mysql::Mysql> for PrimaryKey {
    fn from_sql(value: diesel::mysql::MysqlValue<'_>) -> deserialize::Result<Self> {
        use diesel::{
            mysql::{Mysql, MysqlType},
            sql_types,
        };

        match value.value_type() {
            MysqlType::String => {
                <String as FromSql<sql_types::Text, Mysql>>::from_sql(value).map(PrimaryKey::String)
            }
            // Diesel widens every signed and unsigned width to `i64`, and
            // rejects `BIGINT UNSIGNED` values above `i64::MAX`.
            MysqlType::Tiny
            | MysqlType::UnsignedTiny
            | MysqlType::Short
            | MysqlType::UnsignedShort
            | MysqlType::Long
            | MysqlType::UnsignedLong
            | MysqlType::LongLong
            | MysqlType::UnsignedLongLong => {
                <i64 as FromSql<sql_types::BigInt, Mysql>>::from_sql(value).map(PrimaryKey::from)
            }
            e => Err(format!("Unknown type: {e:?}").into()),
        }
    }
}

impl From<String> for PrimaryKey {
    fn from(s: String) -> Self {
        PrimaryKey::String(s)
    }
}

impl From<i16> for PrimaryKey {
    fn from(i: i16) -> Self {
        PrimaryKey::Integer(i.into())
    }
}

impl From<i32> for PrimaryKey {
    fn from(i: i32) -> Self {
        PrimaryKey::Integer(i.into())
    }
}

impl From<i64> for PrimaryKey {
    fn from(i: i64) -> Self {
        PrimaryKey::Integer(i)
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
            PrimaryKey::Integer(i) => write!(f, "{i}"),
            PrimaryKey::UUID(u) => write!(f, "{u}"),
            PrimaryKey::Composite(pk_vec) => {
                let pk_strings: Vec<String> = pk_vec.iter().map(|pk| format!("{pk}")).collect();
                write!(f, "{}", pk_strings.join(", "))
            }
        }
    }
}
