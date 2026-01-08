//! Submodule defining the `KGLikeDB` trait for knowledge graph-like databases.

use std::io::Write;

use diesel::{PgConnection, QueryDsl, RunQueryDsl, sql_types::Untyped};
use diesel_dynamic_schema::{
    DynamicSelectClause,
    dynamic_value::{DynamicRow, NamedField},
};
use flate2::{Compression, write::GzEncoder};
use sql_traits::traits::{ColumnLike, DatabaseLike, ForeignKeyLike, TableLike};

use crate::{edge_class::EdgeClass, node::Node, primary_key::PrimaryKey};

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

            // If the table has no primary key, we skip it for now.
            if primary_key_columns.is_empty() {
                return Ok(vec![]);
            }

            let dynamic_table = diesel_dynamic_schema::table(table_name);
            let mut select = DynamicSelectClause::new();

            // Store columns and their names to reuse them for selection and ordering
            let columns: Vec<_> = primary_key_columns
                .iter()
                .map(|col| dynamic_table.column::<Untyped, _>(col.column_name()))
                .collect();

            for col in &columns {
                select.add_field(*col);
            }

            let mut query = dynamic_table.select(select).into_boxed();

            for col in &columns {
                query = query.then_order_by(*col);
            }

            let results: Vec<DynamicRow<NamedField<PrimaryKey>>> = query.load(conn)?;

            Ok(results
                .into_iter()
                .map(|row| {
                    let pk_vals = row.into_iter().map(|f| f.value).collect::<Vec<PrimaryKey>>();
                    Node::new(table, pk_vals.into())
                })
                .collect())
        })
    }

    /// Returns the number of nodes in the knowledge graph.
    ///
    /// # Arguments
    ///
    /// * `conn` - A mutable reference to the database connection.
    ///
    /// # Errors
    ///
    /// Returns a `diesel::result::Error` if the database query fails.
    fn number_of_nodes(&self, conn: &mut PgConnection) -> Result<usize, diesel::result::Error> {
        let mut total = 0;

        for table in self.tables() {
            let count: i64 =
                diesel_dynamic_schema::table(table.table_name()).count().get_result(conn)?;

            total += usize::try_from(count).map_err(|_| {
                diesel::result::Error::DeserializationError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Count value too large for usize",
                )))
            })?;
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
    #[allow(clippy::too_many_lines, clippy::type_complexity)]
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
        self.tables()
            .flat_map(move |t| {
                let host_primary_key_columns =
                    t.primary_key_columns(self).collect::<Vec<&Self::Column>>();

                t.foreign_keys(self).filter_map(move |fk| {
                    if !fk.is_referenced_primary_key(self) || host_primary_key_columns.is_empty() {
                        return None;
                    }
                    Some((fk, host_primary_key_columns.clone()))
                })
            })
            .map(move |(fk, host_pk_columns)| {
                // We query the host table to get all rows and their foreign key values,
                // then we create the corresponding nodes for both the host and
                // referenced tables.
                let host_table = fk.host_table(self);
                let referenced_table = fk.referenced_table(self);

                let host_fk_columns = fk.host_columns(self).collect::<Vec<&Self::Column>>();
                let edge_class = EdgeClass::new(host_table, host_fk_columns.clone());

                let dynamic_table = diesel_dynamic_schema::table(host_table.table_name());
                let mut select = DynamicSelectClause::new();

                for col in &host_pk_columns {
                    select.add_field(dynamic_table.column::<Untyped, _>(col.column_name()));
                }
                for col in &host_fk_columns {
                    select.add_field(dynamic_table.column::<Untyped, _>(col.column_name()));
                }

                let results: Vec<DynamicRow<NamedField<PrimaryKey>>> =
                    dynamic_table.select(select).load(conn)?;

                let pk_len = host_pk_columns.len();

                let edges = results
                    .into_iter()
                    .map(|row| {
                        let mut vals =
                            row.into_iter().map(|f| f.value).collect::<Vec<PrimaryKey>>();

                        let fk_vals = vals.split_off(pk_len);
                        let pk_vals = vals;

                        (
                            Node::new(host_table, pk_vals.into()),
                            Node::new(referenced_table, fk_vals.into()),
                            edge_class.clone(),
                        )
                    })
                    .collect::<Vec<_>>();

                Ok(edges)
            })
    }

    /// Writes out the CSVs representing the knowledge graph at the given path.
    ///
    /// # Arguments
    ///
    /// * `conn` - A mutable reference to the database connection.
    /// * `path` - The path where to write the CSV files.
    /// * `compress` - Whether to compress the output files using gzip.
    ///
    /// # Errors
    ///
    /// This function will return an error if the database queries fail or if
    /// writing to the files fails.
    fn write_kg_csvs(
        &self,
        conn: &mut PgConnection,
        path: &std::path::Path,
        compress: bool,
    ) -> Result<(), crate::errors::Error> {
        // If the provided path does not exist, create it.
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        // Write node classes CSV
        let node_classes_path =
            if compress { path.join("node_classes.csv.gz") } else { path.join("node_classes.csv") };
        let file = std::fs::File::create(node_classes_path)?;
        let writer: Box<dyn Write> = if compress {
            Box::new(GzEncoder::new(file, Compression::default()))
        } else {
            Box::new(file)
        };
        let mut write_buffer = std::io::BufWriter::new(writer);
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
        let nodes_path = if compress { path.join("nodes.csv.gz") } else { path.join("nodes.csv") };
        let file = std::fs::File::create(nodes_path)?;
        let writer: Box<dyn Write> = if compress {
            Box::new(GzEncoder::new(file, Compression::default()))
        } else {
            Box::new(file)
        };
        let mut nodes: Vec<Node<'_, Self>> = Vec::with_capacity(self.number_of_nodes(conn)?);
        let mut nodes_writer = std::io::BufWriter::new(writer);
        // Write header
        writeln!(nodes_writer, "node,node_class_ids")?;
        for (table_id, (nodes_result, table)) in self.nodes(conn).zip(self.tables()).enumerate() {
            let table_nodes = nodes_result?;
            let ancestor_table_ids = table
                .ancestral_extended_tables(self)
                .into_iter()
                .map(|t| self.table_id(t).expect("Failed to find tables loaded from the database"))
                .collect::<Vec<usize>>();
            for node in &table_nodes {
                write!(nodes_writer, "\"{node}\",{table_id}")?;
                for ancestor_table_id in &ancestor_table_ids {
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
        let edge_classes_path =
            if compress { path.join("edge_classes.csv.gz") } else { path.join("edge_classes.csv") };
        let file = std::fs::File::create(edge_classes_path)?;
        let writer: Box<dyn Write> = if compress {
            Box::new(GzEncoder::new(file, Compression::default()))
        } else {
            Box::new(file)
        };
        let mut edge_classes_writer = std::io::BufWriter::new(writer);
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
        let edges_path = if compress { path.join("edges.csv.gz") } else { path.join("edges.csv") };
        let file = std::fs::File::create(edges_path)?;
        let writer: Box<dyn Write> = if compress {
            Box::new(GzEncoder::new(file, Compression::default()))
        } else {
            Box::new(file)
        };
        let mut edges_writer = std::io::BufWriter::new(writer);
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
