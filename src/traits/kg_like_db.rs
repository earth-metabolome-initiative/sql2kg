//! Submodule defining the `KGLikeDB` trait for knowledge graph-like databases.

use diesel::{PgConnection, RunQueryDsl, prelude::QueryableByName};
use sql_traits::traits::{ColumnLike, DatabaseLike, ForeignKeyLike, TableLike};
use uuid;

use crate::{edge_class::EdgeClass, node::Node};

/// A trait representing knowledge graph-like database functionalities.
pub trait KGLikeDB: DatabaseLike {
    /// Iterate over the node classes in the knowledge graph.
    ///
    /// # Implementative details
    ///
    /// In a database-based KG, node classes are typically represented as
    /// tables.
    fn node_classes(&self) -> impl Iterator<Item = String> {
        self.tables().map(|table| {
            if let Some(schema) = table.table_schema() {
                format!("{}.{}", schema, table.table_name())
            } else {
                table.table_name().to_string()
            }
        })
    }

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
    fn nodes<'a>(
        &'a self,
        conn: &'a mut PgConnection,
    ) -> impl Iterator<Item = Result<Vec<Node<'a>>, diesel::result::Error>> + 'a {
        self.tables().filter(|table| !table.is_extended(self)).map(move |table| {
            // For each table, we create a SQL diesel query to select the primary key
            // columns and convert them within the query into the standardized
            // node name format.
            let table_schema = table.table_schema();
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
            let primary_key_column_names = primary_key_columns
                .iter()
                .zip(["first", "second", "third"].iter())
                .map(|(col, alias)| format!("\"{}\" as {alias}", col.column_name(),))
                .collect::<Vec<String>>()
                .join(", ");
            let query = diesel::sql_query(format!(
                "SELECT {primary_key_column_names} FROM \"{table_name}\""
            ));

            match column_types.as_slice() {
                ["TEXT" | "VARCHAR"] => {
                    #[derive(QueryableByName)]
                    struct SingleTextPK {
                        #[diesel(sql_type = diesel::sql_types::Text)]
                        first: String,
                    }
                    let results = query.load::<SingleTextPK>(conn)?;
                    Ok(results
                        .into_iter()
                        .map(|row| Node::new(table_schema, table_name, row.first.into()))
                        .collect())
                }
                ["INT"] => {
                    #[derive(QueryableByName)]
                    struct SingleIntegerPK {
                        #[diesel(sql_type = diesel::sql_types::Integer)]
                        first: i32,
                    }
                    let results = query.load::<SingleIntegerPK>(conn)?;
                    Ok(results
                        .into_iter()
                        .map(|row| Node::new(table_schema, table_name, row.first.into()))
                        .collect())
                }
                ["UUID"] => {
                    #[derive(QueryableByName)]
                    struct SingleUuidPK {
                        #[diesel(sql_type = diesel::sql_types::Uuid)]
                        first: uuid::Uuid,
                    }
                    let results = query.load::<SingleUuidPK>(conn)?;
                    Ok(results
                        .into_iter()
                        .map(|row| Node::new(table_schema, table_name, row.first.into()))
                        .collect())
                }
                _ => {
                    unimplemented!(
                        "Primary key column types of {column_types:?} are not yet supported"
                    );
                }
            }
        })
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
    fn edge_classes(&self) -> impl Iterator<Item = EdgeClass<'_>> {
        self.tables().flat_map(move |t| {
            t.foreign_keys(self).filter_map(move |fk| {
                // We disregard foreign keys that do not point to primary key columns
                // in the referenced table.
                if !fk.is_referenced_primary_key(self) {
                    return None;
                }

                let table_schema = t.table_schema();
                let table_name = t.table_name();

                let host_column_names = fk
                    .host_columns(self)
                    .map(sql_traits::traits::ColumnLike::column_name)
                    .collect::<Vec<&str>>();
                Some(EdgeClass::new(table_schema, table_name, host_column_names))
            })
        })
    }

    /// Iterate over the edges in the knowledge graph.
    ///
    /// # Arguments
    ///
    /// * `conn` - A mutable reference to the database connection.
    #[allow(clippy::too_many_lines)]
    fn edges<'a>(
        &'a self,
        conn: &'a mut PgConnection,
    ) -> impl Iterator<Item = Result<Vec<(Node<'a>, Node<'a>, EdgeClass<'a>)>, diesel::result::Error>> + 'a {
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
			let host_table_schema = host_table.table_schema();
			let host_table_name = host_table.table_name();
			let referenced_table = fk.referenced_table(self);
			let referenced_table_schema = referenced_table.table_schema();
			let referenced_table_name = referenced_table.table_name();
			let host_columns = fk.host_columns(self).collect::<Vec<&Self::Column>>();
			let host_column_types = host_columns
				.iter()
				.map(|col| col.normalized_data_type(self))
				.collect::<Vec<&str>>();
			let edge_class = EdgeClass::new(
				host_table_schema,
				host_table_name,
				host_columns
					.iter()
					.map(|col| col.column_name())
					.collect(),
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
								Node::new(host_table_schema, host_table_name, row.first?.into()),
								Node::new(referenced_table_schema, referenced_table_name, row.first_host?.into()),
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
								Node::new(host_table_schema, host_table_name, row.first?.into()),
								Node::new(referenced_table_schema, referenced_table_name, row.first_host?.into()),
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
								Node::new(host_table_schema, host_table_name, row.first?.into()),
								Node::new(referenced_table_schema, referenced_table_name, row.first_host?.into()),
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
								Node::new(host_table_schema, host_table_name, row.first?.into()),
								Node::new(referenced_table_schema, referenced_table_name, row.first_host?.into()),
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
								Node::new(host_table_schema, host_table_name, row.first?.into()),
								Node::new(referenced_table_schema, referenced_table_name, row.first_host?.into()),
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
								Node::new(host_table_schema, host_table_name, row.first?.into()),
								Node::new(referenced_table_schema, referenced_table_name, row.first_host?.into()),
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
								Node::new(host_table_schema, host_table_name, row.first?.into()),
								Node::new(referenced_table_schema, referenced_table_name, row.first_host?.into()),
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
        let mut node_classes_writer = csv::Writer::from_path(node_classes_path)?;
        // Write header
        node_classes_writer.write_record(&["class_name"])?;

        for class_name in self.node_classes() {
            node_classes_writer.write_record(&[class_name])?;
        }
        node_classes_writer.flush()?;

        // Write nodes CSV
        let nodes_path = path.join("nodes.csv");
        let mut nodes_writer = csv::Writer::from_path(nodes_path)?;
        // Write header
        nodes_writer.write_record(&["node_id", "class_names"])?;
        for nodes_result in self.nodes(conn) {
            let nodes = nodes_result?;
            for node in nodes {
                let node_id = node.to_string();
                let node_table = self.table(node.schema_name(), node.table_name()).unwrap();
                let ancestor_tables = node_table.ancestral_extended_tables(self);
                let mut class_names = vec![node.class_name()];
                for ancestor in ancestor_tables {
                    let ancestor_schema = ancestor.table_schema();
                    let ancestor_name = ancestor.table_name();
                    if let Some(schema) = ancestor_schema {
                        class_names.push(format!("{}.{}", schema, ancestor_name));
                    } else {
                        class_names.push(ancestor_name.to_string());
                    }
                }
                let class_names_str = class_names.join("|");
                nodes_writer.write_record(&[node_id, class_names_str])?;
            }
        }
        nodes_writer.flush()?;

		// Write edge classes CSV
		let edge_classes_path = path.join("edge_classes.csv");
		let mut edge_classes_writer = csv::Writer::from_path(edge_classes_path)?;
		// Write header
		edge_classes_writer.write_record(&["edge_class"])?;
		for edge_class in self.edge_classes() {
			edge_classes_writer.write_record(&[edge_class.to_string()])?;
		}
		edge_classes_writer.flush()?;

        // Write edges CSV
        let edges_path = path.join("edges.csv");
        let mut edges_writer = csv::Writer::from_path(edges_path)?;
        // Write header
        edges_writer.write_record(&["source", "destination", "edge_class"])?;
        for edges_result in self.edges(conn) {
            let edges = edges_result?;
            for (host_node, referenced_node, edge_class) in edges {
                edges_writer.write_record(&[host_node.to_string(), referenced_node.to_string(), edge_class.to_string()])?;
            }
        }
        edges_writer.flush()?;

        Ok(())
    }
}

impl<KG> KGLikeDB for KG where KG: DatabaseLike {}
