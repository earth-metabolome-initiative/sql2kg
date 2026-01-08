//! Submodule defining the `KGLikeDB` trait for knowledge graph-like databases.

use std::io::Write;

use diesel::{PgConnection, RunQueryDsl, prelude::QueryableByName};
use sql_traits::traits::{ColumnLike, DatabaseLike, ForeignKeyLike, TableLike};
use uuid;

use crate::{edge_class::EdgeClass, node::Node};

/// A trait representing knowledge graph-like database functionalities.
pub trait KGLikeDB: DatabaseLike {
    /// Iterate over the nodes in the knowledge graph.
    ///
    /// # Arguments
    ///
    /// * `conn` - A reference to the database connection.
    ///
    /// # Implementative details
    ///
    /// Each node is a row in any table of the database and is represented by
    /// the name of the table and the primary key value of the row, such as:
    ///
    /// ```plain
    /// users(1)
    /// comments(3995db4d-2b2d-4c0e-8c5f-eeeb1efbd315, 8b1756b7-58b8-40cc-81b3-46ba68c8e964)
    /// ```
    ///
    /// In order to avoid duplicated nodes, if a table is a descendant of
    /// another table in an inheritance hierarchy, only the rows of the most
    /// derived tables are returned, i.e. only the nodes of a leaf table are
    /// returned.
    fn nodes<'conn, 'db>(
        &'db self,
        conn: &'conn mut PgConnection,
    ) -> impl Iterator<Item = Result<Vec<Node<'db, Self>>, diesel::result::Error>> + 'conn
    where
        'db: 'conn,
    {
        self.tables().filter(|table| !table.is_extended(self)).map(move |table| {
            // For each table, we create a SQL diesel query to select the primary key
            // columns and convert them within the query into the standardized
            // node name format.

            let table_name = table.table_name();
            let primary_key_columns =
                table.primary_key_columns(self).collect::<Vec<&Self::Column>>();

            // If the table has no primary key or has more than 3 primary key columns, we
            // skip it for now.
            if primary_key_columns.is_empty() || primary_key_columns.len() > 3 {
                return Ok(vec![]);
            }

            let column_types = primary_key_columns
                .iter()
                .map(|col| col.normalized_data_type(self))
                .collect::<Vec<&str>>();
            let aliases = ["first", "second", "third"];
            let primary_key_column_names = primary_key_columns
                .iter()
                .zip(aliases.iter())
                .map(|(col, alias)| format!("\"{}\" as {alias}", col.column_name(),))
                .collect::<Vec<String>>()
                .join(", ");
            let primary_key_aliases = primary_key_columns.iter().map(|col| if col.is_textual(self) {
                  format!("\"{}\" COLLATE \"C\"", col.column_name())
                } else {
                  format!("\"{}\"", col.column_name())
                }).collect::<Vec<String>>().join(", ");

            let query = diesel::sql_query(format!(
                "SELECT {primary_key_column_names} FROM \"{table_name}\" ORDER BY {primary_key_aliases}"
            ));

            match column_types.as_slice() {
                ["TEXT" | "VARCHAR"] => {
                    #[derive(QueryableByName)]
                    struct SingleTextPK {
                        #[diesel(sql_type = diesel::sql_types::Text)]
                        first: String,
                    }
                    let results = query.load::<SingleTextPK>(conn)?;
                    Ok(results.into_iter().map(|row| Node::new(table, row.first.into())).collect())
                }
                ["INT"] => {
                    #[derive(QueryableByName)]
                    struct SingleIntegerPK {
                        #[diesel(sql_type = diesel::sql_types::Integer)]
                        first: i32,
                    }
                    let results = query.load::<SingleIntegerPK>(conn)?;
                    Ok(results.into_iter().map(|row| Node::new(table, row.first.into())).collect())
                }
                ["UUID"] => {
                    #[derive(QueryableByName)]
                    struct SingleUuidPK {
                        #[diesel(sql_type = diesel::sql_types::Uuid)]
                        first: uuid::Uuid,
                    }
                    let results = query.load::<SingleUuidPK>(conn)?;
                    Ok(results.into_iter().map(|row| Node::new(table, row.first.into())).collect())
                }
                _ => {
                    unimplemented!(
                        "Primary key column types of {column_types:?} are not yet supported"
                    );
                }
            }
        })
    }

    /// Returns the number of nodes in the knowledge graph.
    ///
    /// # Arguments
    ///
    /// * `conn` - A mutable reference to the database connection.
    fn number_of_nodes(&self, conn: &mut PgConnection) -> Result<usize, diesel::result::Error> {
        let mut total = 0;

        #[derive(QueryableByName)]
        struct Count {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            count: i64,
        }

        for table in self.tables() {
            total += diesel::sql_query(format!(
                "SELECT COUNT(*) as count FROM \"{}\"",
                table.table_name()
            ))
            .get_result::<Count>(conn)?
            .count as usize;
        }
        Ok(total)
    }

    /// Iterate over the edges classes in the knowledge graph.
    ///
    /// # Implementative details
    ///
    /// An edge in this context is a foreign key relationship between two
    /// tables, based on some host table's foreign key columns pointing to a
    /// referenced table's primary key columns. Each edge class is represented
    /// as a tuple of the host table name, the referenced table name, and
    /// the foreign key column names.
    fn edge_classes(&self) -> impl Iterator<Item = EdgeClass<'_, Self>> {
        self.tables().flat_map(move |t| {
            let mut edge_classes = t
                .foreign_keys(self)
                .filter_map(move |fk| {
                    // We disregard foreign keys that do not point to primary key columns
                    // in the referenced table.
                    if !fk.is_referenced_primary_key(self) {
                        return None;
                    }

                    let host_columns = fk.host_columns(self).collect::<Vec<_>>();
                    Some(EdgeClass::new(t, host_columns))
                })
                .collect::<Vec<EdgeClass<'_, Self>>>();
            edge_classes.sort_unstable();
            edge_classes
        })
    }

    /// Iterate over the edges in the knowledge graph.
    ///
    /// # Arguments
    ///
    /// * `conn` - A mutable reference to the database connection.
    #[allow(clippy::too_many_lines)]
    fn edges<'conn, 'db>(
        &'db self,
        conn: &'conn mut PgConnection,
    ) -> impl Iterator<
        Item = Result<
            Vec<(Node<'db, Self>, Node<'db, Self>, EdgeClass<'db, Self>)>,
            diesel::result::Error,
        >,
    > + 'conn
    where
        'db: 'conn,
    {
        self.tables().flat_map(move |t| {
            let host_primary_key_columns =
                t.primary_key_columns(self).collect::<Vec<&Self::Column>>();

            let host_pk_column_types = host_primary_key_columns
                .iter()
                .map(|col| col.normalized_data_type(self))
                .collect::<Vec<&str>>();
            let host_pk_column_names = host_primary_key_columns
                .iter()
                .zip(["first", "second", "third"].iter())
                .map(|(col, alias)| format!("\"{}\" as {alias}", col.column_name(),))
                .collect::<Vec<String>>()
                .join(", ");

			t.foreign_keys(self).filter_map(move |fk| {
				if !fk.is_referenced_primary_key(self)
                    || host_primary_key_columns.is_empty()
                    || host_primary_key_columns.len() > 3
                {
                    return None;
                }
					Some((fk, host_pk_column_types.clone(), host_pk_column_names.clone()))
			})
        }).map(move |(fk, host_pk_column_types, host_pk_column_names)| {
			// We query the host table to get all rows and their foreign key values,
			// then we create the corresponding nodes for both the host and
			// referenced tables.
			let host_table = fk.host_table(self);
			let _host_table_schema = host_table.table_schema();
			let host_table_name = host_table.table_name();
			let referenced_table = fk.referenced_table(self);
			let _referenced_table_schema = referenced_table.table_schema();
			let _referenced_table_name = referenced_table.table_name();
			let host_columns = fk.host_columns(self).collect::<Vec<&Self::Column>>();
			let host_column_types = host_columns
				.iter()
				.map(|col| col.normalized_data_type(self))
				.collect::<Vec<&str>>();
			let edge_class = EdgeClass::new(
				host_table,
				host_columns.clone(),
			);

			let host_column_names = host_columns
				.iter()
				.zip(["first_host", "second_host", "third_host"].iter())
				.map(|(col, alias)| format!("\"{}\" as {alias}", col.column_name(),))
				.collect::<Vec<String>>()
				.join(", ");

			let query = diesel::sql_query(format!(
				"SELECT {host_pk_column_names}, {host_column_names} FROM \"{host_table_name}\""
			));

			match (host_pk_column_types.as_slice(), host_column_types.as_slice()) {
				(["TEXT" | "VARCHAR"], ["TEXT" | "VARCHAR"]) => {
					#[derive(QueryableByName)]
					struct TextToText {
						#[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
						first: Option<String>,
						#[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
						first_host: Option<String>,
					}
					let results = query.load::<TextToText>(conn)?;
					Ok(results
						.into_iter()
						.filter_map(|row| {
							Some((
								Node::new(host_table, row.first?.into()),
								Node::new(referenced_table, row.first_host?.into()),
								edge_class.clone()
							))
						})
						.collect())
				}
				(["INT"], ["INT"]) => {
					#[derive(QueryableByName)]
					struct IntToInt {
						#[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
						first: Option<i32>,
						#[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
						first_host: Option<i32>,
					}
					let results = query.load::<IntToInt>(conn)?;
					Ok(results
						.into_iter()
						.filter_map(|row| {
							Some((
								Node::new(host_table, row.first?.into()),
								Node::new(referenced_table, row.first_host?.into()),
								edge_class.clone()
							))
						})
						.collect())
				}
				(["UUID"], ["UUID"]) => {
					#[derive(QueryableByName)]
					struct UuidToUuid {
						#[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
						first: Option<uuid::Uuid>,
						#[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
						first_host: Option<uuid::Uuid>,
					}
					let results = query.load::<UuidToUuid>(conn)?;
					Ok(results
						.into_iter()
						.filter_map(|row| {
							Some((
								Node::new(host_table, row.first?.into()),
								Node::new(referenced_table, row.first_host?.into()),
								edge_class.clone()
							))
						})
						.collect())
				}
				(["INT"], ["UUID"]) => {
					#[derive(QueryableByName)]
					struct IntToUuid {
						#[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
						first: Option<i32>,
						#[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
						first_host: Option<uuid::Uuid>,
					}
					let results = query.load::<IntToUuid>(conn)?;
					Ok(results
						.into_iter()
						.filter_map(|row| {
							Some((
								Node::new(host_table, row.first?.into()),
								Node::new(referenced_table, row.first_host?.into()),
								edge_class.clone()
							))
						})
						.collect())
				}
				(["UUID"], ["INT"]) => {
					#[derive(QueryableByName)]
					struct UuidToInt {
						#[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
						first: Option<uuid::Uuid>,
						#[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
						first_host: Option<i32>,
					}
					let results = query.load::<UuidToInt>(conn)?;
					Ok(results
						.into_iter()
						.filter_map(|row| {
							Some((
								Node::new(host_table, row.first?.into()),
								Node::new(referenced_table, row.first_host?.into()),
								edge_class.clone()
							))
						})
						.collect())
				}
				(["VARCHAR"], ["UUID"]) => {
					#[derive(QueryableByName)]
					struct VarcharToUuid {
						#[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
						first: Option<String>,
						#[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
						first_host: Option<uuid::Uuid>,
					}
					let results = query.load::<VarcharToUuid>(conn)?;
					Ok(results
						.into_iter()
						.filter_map(|row| {
							Some((
								Node::new(host_table, row.first?.into()),
								Node::new(referenced_table, row.first_host?.into()),
								edge_class.clone()
							))
						})
						.collect())
				}
				(["UUID"], ["VARCHAR"]) => {
					#[derive(QueryableByName)]
					struct UuidToVarchar {
						#[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
						first: Option<uuid::Uuid>,
						#[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
						first_host: Option<String>,
					}
					let results = query.load::<UuidToVarchar>(conn)?;
					Ok(results
						.into_iter()
						.filter_map(|row| {
							Some((
								Node::new(host_table, row.first?.into()),
								Node::new(referenced_table, row.first_host?.into()),
								edge_class.clone()
							))
						})
						.collect())
				}
				_ => {
					unimplemented!(
						"Primary key column types of host {host_pk_column_types:?} and foreign key column types of host {host_column_types:?} are not yet supported"
					);
				}
			}
		})
    }

    /// Writes out the CSVs representing the knowledge graph at the given path.
    ///
    /// # Arguments
    ///
    /// * `conn` - A mutable reference to the database connection.
    /// * `path` - The path where to write the CSV files.
    ///
    /// # Errors
    ///
    /// This function will return an error if the database queries fail or if
    /// writing to the files fails.
    fn write_kg_csvs(
        &self,
        conn: &mut PgConnection,
        path: &std::path::Path,
    ) -> Result<(), crate::errors::Error> {
        // If the provided path does not exist, create it.
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        // Write node classes CSV
        let node_classes_path = path.join("node_classes.csv");
        let file = std::fs::File::create(node_classes_path)?;
        let mut write_buffer = std::io::BufWriter::new(file);
        // Write header
        writeln!(write_buffer, "node_class")?;
        for table in self.tables() {
            let table_schema = table.table_schema();
            let table_name = table.table_name();
            if let Some(schema) = table_schema {
                writeln!(write_buffer, "\"{schema}.{table_name}\"")?;
            } else {
                writeln!(write_buffer, "\"{table_name}\"")?;
            }
        }
        write_buffer.flush()?;

        // Write nodes CSV
        let nodes_path = path.join("nodes.csv");
        let file = std::fs::File::create(nodes_path)?;
        let mut nodes: Vec<Node<'_, Self>> = Vec::with_capacity(self.number_of_nodes(conn)?);
        let mut nodes_writer = std::io::BufWriter::new(file);
        // Write header
        writeln!(nodes_writer, "node,node_class_ids")?;
        for (table_id, (nodes_result, table)) in self.nodes(conn).zip(self.tables()).enumerate() {
            let table_nodes = nodes_result?;
            let ancestor_table_ids = table
                .ancestral_extended_tables(self)
                .into_iter()
                .map(|t| self.table_id(t).expect("Failed to find tables loaded from the database"))
                .collect::<Vec<usize>>();
            for node in table_nodes.iter() {
                write!(nodes_writer, "\"{node}\",{table_id}")?;
                for ancestor_table_id in ancestor_table_ids.iter() {
                    write!(nodes_writer, "|{ancestor_table_id}")?;
                }
                writeln!(nodes_writer)?;
            }
            nodes.extend(table_nodes);
        }
        nodes_writer.flush()?;

        // Since the tables are sorted and the nodes themselves are sorted within
        // each table, the nodes are globally sorted.
        debug_assert!(nodes.windows(2).all(|w| w[0] <= w[1]), "Nodes are not sorted");

        // Write edge classes CSV
        let edge_classes_path = path.join("edge_classes.csv");
        let file = std::fs::File::create(edge_classes_path)?;
        let mut edge_classes_writer = std::io::BufWriter::new(file);
        let mut edge_classes: Vec<EdgeClass<'_, Self>> = Vec::new();
        // Write header
        writeln!(edge_classes_writer, "edge_class")?;
        for edge_class in self.edge_classes() {
            writeln!(edge_classes_writer, "\"{edge_class}\"")?;
            edge_classes.push(edge_class);
        }
        edge_classes_writer.flush()?;

        // Since the edge classes are sorted, we can assert that here.
        debug_assert!(edge_classes.windows(2).all(|w| w[0] <= w[1]), "Edge classes are not sorted");

        // Write edges CSV
        let edges_path = path.join("edges.csv");
        let file = std::fs::File::create(edges_path)?;
        let mut edges_writer = std::io::BufWriter::new(file);
        // Write header
        writeln!(edges_writer, "src_id,dst_id,edge_class_id")?;
        for edges_result in self.edges(conn) {
            let edges = edges_result?;
            for (host_node, referenced_node, edge_class) in edges {
                let src_id = nodes
                    .binary_search(&host_node)
                    .map_err(|_| crate::errors::Error::NodeNotFound(host_node.to_string()))?;
                let dst_id = nodes
                    .binary_search(&referenced_node)
                    .map_err(|_| crate::errors::Error::NodeNotFound(referenced_node.to_string()))?;
                let edge_class_id = edge_classes
                    .binary_search(&edge_class)
                    .map_err(|_| crate::errors::Error::EdgeClassNotFound(edge_class.to_string()))?;
                writeln!(edges_writer, "{src_id},{dst_id},{edge_class_id}")?;
            }
        }
        edges_writer.flush()?;

        Ok(())
    }
}

impl<KG> KGLikeDB for KG where KG: DatabaseLike {}
