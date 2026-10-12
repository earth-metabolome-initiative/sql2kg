//! Submodule defining the `KgConnection` trait, the database access needed to
//! extract a knowledge graph.

use crate::primary_key::PrimaryKey;

/// A database connection able to run the queries needed to extract a
/// knowledge graph.
///
/// Implemented for the diesel connection of every enabled backend:
/// `PgConnection` (feature `postgres`), `SqliteConnection` (feature `sqlite`)
/// and `MysqlConnection` (feature `mysql`).
pub trait KgConnection {
    /// Returns the number of rows of a table.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema of the table, if it has one.
    /// * `table` - The name of the table.
    ///
    /// # Errors
    ///
    /// Returns a `diesel::result::Error` if the database query fails.
    fn count_rows(&mut self, schema: Option<&str>, table: &str) -> diesel::QueryResult<i64>;

    /// Returns the values of the given columns for every row of a table,
    /// in the order the columns are given.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema of the table, if it has one.
    /// * `table` - The name of the table.
    /// * `columns` - The names of the columns to load.
    /// * `require_not_null` - Whether to skip the rows where any of the columns
    ///   is `NULL`.
    ///
    /// # Errors
    ///
    /// Returns a `diesel::result::Error` if the database query fails, or if
    /// a value cannot be decoded as a `PrimaryKey`.
    fn load_keys(
        &mut self,
        schema: Option<&str>,
        table: &str,
        columns: &[&str],
        require_not_null: bool,
    ) -> diesel::QueryResult<Vec<Vec<PrimaryKey>>>;
}

/// Returns the dynamic table with the given name, qualified by its schema if
/// it has one.
#[cfg(any(feature = "postgres", feature = "sqlite", feature = "mysql"))]
fn dynamic_table<'a>(
    schema: Option<&'a str>,
    table: &'a str,
) -> diesel_dynamic_schema::Table<&'a str> {
    match schema {
        Some(schema) => diesel_dynamic_schema::schema(schema).table(table),
        None => diesel_dynamic_schema::table(table),
    }
}

/// Implements `KgConnection` for concrete diesel connections, so that the
/// queries type-check against one backend at a time instead of requiring
/// backend-generic diesel bounds.
#[cfg(any(feature = "postgres", feature = "sqlite", feature = "mysql"))]
macro_rules! impl_kg_connection {
    ($connection:ty) => {
        impl KgConnection for $connection {
            fn count_rows(
                &mut self,
                schema: Option<&str>,
                table: &str,
            ) -> diesel::QueryResult<i64> {
                use diesel::{QueryDsl, RunQueryDsl};

                dynamic_table(schema, table).count().get_result(self)
            }

            fn load_keys(
                &mut self,
                schema: Option<&str>,
                table: &str,
                columns: &[&str],
                require_not_null: bool,
            ) -> diesel::QueryResult<Vec<Vec<PrimaryKey>>> {
                use diesel::{QueryDsl, RunQueryDsl, UntypedExpressionMethods, sql_types::Untyped};
                use diesel_dynamic_schema::{
                    DynamicSelectClause,
                    dynamic_value::{DynamicRow, NamedField},
                };

                let dynamic_table = dynamic_table(schema, table);
                let columns = columns
                    .iter()
                    .map(|column| dynamic_table.column::<Untyped, _>(*column))
                    .collect::<Vec<_>>();
                let select = columns.iter().collect::<DynamicSelectClause<_, _>>();

                // The query is boxed so that the filters can be added
                // dynamically.
                let mut query = dynamic_table.select(select).into_boxed();
                if require_not_null {
                    for column in &columns {
                        query = query.filter(column.is_not_null());
                    }
                }

                let rows: Vec<DynamicRow<NamedField<PrimaryKey>>> = query.load(self)?;
                Ok(rows.into_iter().map(Vec::from).collect())
            }
        }
    };
}

#[cfg(feature = "postgres")]
impl_kg_connection!(diesel::PgConnection);
#[cfg(feature = "sqlite")]
impl_kg_connection!(diesel::SqliteConnection);
#[cfg(feature = "mysql")]
impl_kg_connection!(diesel::MysqlConnection);
