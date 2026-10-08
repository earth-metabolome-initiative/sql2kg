//! Submodule defining the `KGLikeDB` trait for knowledge graph-like databases.

use std::io::Write;

use sql_traits::{
    errors::LookupError,
    traits::{ColumnLike, DatabaseLike, ForeignKeyLike, TableLike},
};
use time_requirements::{prelude::TimeTracker, task::Task};

use crate::{edge_class::EdgeClass, node::Node, traits::KgConnection};

/// A trait representing knowledge graph-like database functionalities.
pub trait KGLikeDB: DatabaseLike {
    /// Iterate over the nodes in the knowledge graph, one batch per table
    /// with a primary key.
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
    ///
    /// # Errors
    ///
    /// Returns a `LookupError` if the primary key of a table cannot be
    /// resolved in the schema. Each batch is a `diesel::result::Error` if its
    /// database query fails.
    #[allow(clippy::type_complexity)]
    fn nodes<'conn, 'db, C: KgConnection>(
        &'db self,
        conn: &'conn mut C,
    ) -> Result<impl Iterator<Item = diesel::QueryResult<Vec<Node<'db, Self>>>> + 'conn, LookupError>
    where
        'db: 'conn,
    {
        // Resolve every primary key upfront, so that schema errors surface
        // before any query runs.
        let mut tables = Vec::new();
        for table in self.tables() {
            let primary_key_columns =
                table.primary_key_columns(self)?.map(ColumnLike::column_name).collect::<Vec<_>>();
            if !primary_key_columns.is_empty() {
                tables.push((table, primary_key_columns));
            }
        }

        Ok(tables.into_iter().map(move |(table, primary_key_columns)| {
            let mut nodes: Vec<Node<'_, Self>> = conn
                .load_keys(table.table_schema(), table.table_name(), &primary_key_columns, false)?
                .into_iter()
                .map(|primary_keys| Node::new(table, primary_keys.into()))
                .collect();

            // Ideally, the nodes should be queries with `ORDER BY` clause to
            // ensure consistent ordering, but since we cannot guarantee that
            // all primary key columns can be ordered using the expected
            // collation, we sort them in Rust instead.
            nodes.sort_unstable();

            Ok(nodes)
        }))
    }

    /// Returns the number of nodes in the knowledge graph.
    ///
    /// # Arguments
    ///
    /// * `conn` - A mutable reference to the database connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the primary key of a table cannot be resolved in
    /// the schema, or if a database query fails.
    fn number_of_nodes(&self, conn: &mut impl KgConnection) -> Result<usize, crate::errors::Error> {
        let mut total = 0;

        for table in self.tables() {
            if !table.has_primary_key(self)? {
                continue;
            }

            let count = conn.count_rows(table.table_schema(), table.table_name())?;

            total += usize::try_from(count).map_err(|_| {
                diesel::result::Error::DeserializationError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Count value too large for usize",
                )))
            })?;
        }
        Ok(total)
    }

    /// Returns the edges classes in the knowledge graph.
    ///
    /// # Implementative details
    ///
    /// An edge in this context is a foreign key relationship between two
    /// tables, based on some host table's foreign key columns pointing to a
    /// referenced table's primary key columns. Each edge class is represented
    /// as a tuple of the host table name, the referenced table name, and
    /// the foreign key column names.
    ///
    /// # Errors
    ///
    /// Returns a `LookupError` if a foreign key cannot be resolved in the
    /// schema.
    fn edge_classes(&self) -> Result<Vec<EdgeClass<'_, Self>>, LookupError> {
        let mut edge_classes = Vec::new();
        for t in self.tables() {
            let first = edge_classes.len();
            for fk in t.foreign_keys(self)? {
                // We disregard foreign keys that do not point to primary
                // key columns in the referenced table.
                if fk.is_referenced_primary_key(self)? {
                    edge_classes.push(EdgeClass::new(t, fk.host_columns(self)?.collect()));
                }
            }
            edge_classes[first..].sort_unstable();
        }
        Ok(edge_classes)
    }

    /// Iterate over the edges in the knowledge graph, one batch per foreign
    /// key.
    ///
    /// # Arguments
    ///
    /// * `conn` - A mutable reference to the database connection.
    ///
    /// # Errors
    ///
    /// Returns a `LookupError` if a primary or foreign key cannot be resolved
    /// in the schema. Each batch is a `diesel::result::Error` if its database
    /// query fails.
    #[allow(clippy::type_complexity)]
    fn edges<'conn, 'db, C: KgConnection>(
        &'db self,
        conn: &'conn mut C,
    ) -> Result<
        impl Iterator<
            Item = diesel::QueryResult<
                Vec<(Node<'db, Self>, Node<'db, Self>, EdgeClass<'db, Self>)>,
            >,
        > + 'conn,
        LookupError,
    >
    where
        'db: 'conn,
    {
        // Resolve every foreign key upfront, so that schema errors surface
        // before any query runs.
        let mut foreign_keys = Vec::new();
        for t in self.tables() {
            let host_pk_columns = t.primary_key_columns(self)?.collect::<Vec<&Self::Column>>();
            if host_pk_columns.is_empty() {
                continue;
            }
            for fk in t.foreign_keys(self)? {
                if !fk.is_referenced_primary_key(self)? {
                    continue;
                }
                let host_table = fk.host_table(self);
                let referenced_table = fk.referenced_table(self)?;
                let host_fk_columns = fk.host_columns(self)?.collect::<Vec<&Self::Column>>();
                // The query selects the host primary key columns followed by
                // the foreign key columns.
                let columns = host_pk_columns
                    .iter()
                    .chain(host_fk_columns.iter())
                    .copied()
                    .map(ColumnLike::column_name)
                    .collect::<Vec<_>>();
                let edge_class = EdgeClass::new(host_table, host_fk_columns);
                foreign_keys.push((
                    host_table,
                    referenced_table,
                    columns,
                    host_pk_columns.len(),
                    edge_class,
                ));
            }
        }

        Ok(foreign_keys.into_iter().map(
            move |(host_table, referenced_table, columns, pk_len, edge_class)| {
                // We query the host table to get all rows and their foreign
                // key values, skipping the rows where any of the involved
                // columns is null, then we create the corresponding nodes for
                // both the host and referenced tables.
                let rows = conn.load_keys(
                    host_table.table_schema(),
                    host_table.table_name(),
                    &columns,
                    true,
                )?;

                let edges = rows
                    .into_iter()
                    .map(|mut pk_vals| {
                        let fk_vals = pk_vals.split_off(pk_len);

                        (
                            Node::new(host_table, pk_vals.into()),
                            Node::new(referenced_table, fk_vals.into()),
                            edge_class.clone(),
                        )
                    })
                    .collect::<Vec<_>>();

                Ok(edges)
            },
        ))
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
        conn: &mut impl KgConnection,
        path: &std::path::Path,
    ) -> Result<TimeTracker, crate::errors::Error> {
        // If the provided path does not exist, create it.
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let mut tracker = TimeTracker::new("Write KG CSVs");
        let task = Task::new("Writing node class CSV");
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
        tracker.add_completed_task(task);

        let task = Task::new("Writing node CSV");
        // Write nodes CSV
        let nodes_path = path.join("nodes.csv");
        let file = std::fs::File::create(nodes_path)?;
        let mut nodes: Vec<Node<'_, Self>> = Vec::with_capacity(self.number_of_nodes(conn)?);
        let mut nodes_writer = std::io::BufWriter::new(file);
        // Write header
        writeln!(nodes_writer, "node,node_class_ids")?;
        for (table_id, (nodes_result, table)) in self.nodes(conn)?.zip(self.tables()).enumerate() {
            let table_nodes = nodes_result?;
            let ancestor_table_ids = table
                .ancestral_extended_tables(self)?
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
        tracker.add_completed_task(task);

        // Since the tables are sorted and the nodes themselves are sorted
        // within each table, the nodes are globally sorted.
        debug_assert!(nodes.windows(2).all(|w| w[0] <= w[1]), "Nodes are not sorted");

        let task = Task::new("Writing edge classes CSV");
        // Write edge classes CSV
        let edge_classes_path = path.join("edge_classes.csv");
        let file = std::fs::File::create(edge_classes_path)?;
        let mut edge_classes_writer = std::io::BufWriter::new(file);
        let mut edge_classes: Vec<EdgeClass<'_, Self>> = Vec::new();
        // Write header
        writeln!(edge_classes_writer, "edge_class")?;
        for edge_class in self.edge_classes()? {
            writeln!(edge_classes_writer, "\"{edge_class}\"")?;
            edge_classes.push(edge_class);
        }
        edge_classes_writer.flush()?;
        tracker.add_completed_task(task);

        // Since the edge classes are sorted, we can assert that here.
        debug_assert!(edge_classes.windows(2).all(|w| w[0] <= w[1]), "Edge classes are not sorted");

        let task = Task::new("Writing edges CSV");
        // Write edges CSV
        let edges_path = path.join("edges.csv");
        let file = std::fs::File::create(edges_path)?;
        let mut edges_writer = std::io::BufWriter::new(file);
        // Write header
        writeln!(edges_writer, "src_id,dst_id,edge_class_id")?;
        for edges_result in self.edges(conn)? {
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
        tracker.add_completed_task(task);

        Ok(tracker)
    }
}

impl<KG> KGLikeDB for KG where KG: DatabaseLike {}
