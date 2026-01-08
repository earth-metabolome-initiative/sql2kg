# SQL2KG

[![CI](https://github.com/earth-metabolome-initiative/sql2kg/workflows/Rust%20CI/badge.svg)](https://github.com/earth-metabolome-initiative/sql2kg/actions)
[![Security Audit](https://github.com/earth-metabolome-initiative/sql2kg/workflows/Security%20Audit/badge.svg)](https://github.com/earth-metabolome-initiative/sql2kg/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Codecov](https://codecov.io/gh/earth-metabolome-initiative/sql2kg/branch/main/graph/badge.svg)](https://codecov.io/gh/earth-metabolome-initiative/sql2kg)

A Rust library for converting SQL databases into Knowledge Graph representations.

This library leverages the [`sql-traits`](https://github.com/earth-metabolome-initiative/sql-traits) generic schema abstraction to inspect SQL databases and extract nodes and edges, exporting them to standardized CSV formats.

## Features

* **Automated Extraction**: Automatically converts SQL tables to nodes and foreign keys to edges.
* **Schema Agnostic**: Operates on any database backend implementing the `sql-traits` interfaces.
* **Generic Output**: Writes to standard CSV files (`nodes.csv`, `edges.csv`, `node_classes.csv`).
* **Diesel Integration**: Built on top of [`diesel`](https://diesel.rs) for robust database interaction.

## Usage

This crate is currently not published to `crates.io`. You can add it to your project via git:

```toml
[dependencies]
sql2kg = { git = "https://github.com/earth-metabolome-initiative/sql2kg" }
```

## Example

The following example demonstrates how to use the `KGLikeDB` trait to export Knowledge Graph CSVs from a database schema wrapper.

```rust,no_run
use diesel::PgConnection;
use sql2kg::prelude::*;
use std::path::Path;

/// Exports knowledge graph, provided a schema wrapper and a connection.
///
/// # Arguments
/// * `db_schema` - An object implementing `KGLikeDB` (and `DatabaseLike`)
/// * `conn` - Active PostgreSQL connection
fn export_to_csv<DB>(
    db_schema: &DB,
    conn: &mut PgConnection
) -> Result<(), Box<dyn std::error::Error>> 
where
    DB: KGLikeDB
{
    let output_path = Path::new("./kg_output");
    db_schema.write_kg_csvs(conn, output_path)?;
    Ok(())
}
```

## Real-world Examples

This library is used in the [directus-schema-models](https://github.com/earth-metabolome-initiative/directus-schema-models) project to export knowledge graphs from Directus database schemas.

### Knowledge Graph Extraction (Rust)

The following example illustrates how to set up the database introspection (via [`pg_diesel`](https://github.com/earth-metabolome-initiative/pg_diesel)) and export the Knowledge Graph CSVs using `sql2kg`.
This code is adapted from the `builder` executable in the [directus-schema-models](https://github.com/earth-metabolome-initiative/directus-schema-models) repository.

```rust,no_run
use diesel::{Connection, PgConnection};
use pg_diesel::database::{PgDieselDatabase, PgDieselDatabaseBuilder};
use sql2kg::prelude::*;
use std::path::PathBuf;

/// Executable to generate the code for the Directus database.
pub fn main() {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mut conn = PgConnection::establish(&database_url)
        .expect("Failed to connect to database");

    let db: PgDieselDatabase = PgDieselDatabaseBuilder::default()
        .connection(&mut conn)
        .schema("public")
        .try_into()
        .expect("Failed to build database");

    let kg_path = PathBuf::from("kg_data/directus");
    
    // Write CSVs without compression (false flag)
    db.write_kg_csvs(&mut conn, kg_path.as_path(), false)
        .expect("Failed to write KG CSVs");
}
```

### Knowledge Graph Analysis (Python)

Once the CSVs are generated, they can be loaded and analyzed using the [Grape](https://github.com/AnacletoLAB/grape) library.

Prerequisites:

```bash
pip install grape
```

```python
"""Loads and analyzes the knowledge graph from CSV files."""
from grape import Graph

def main():
    """Loads and analyzes the knowledge graph from CSV files."""
    # Load the knowledge graph from CSV files
    kg = Graph.from_csv(
        directed=False,
        node_type_path="kg_data/directus/node_classes.csv",
        node_types_column="node_class",
        node_path="kg_data/directus/nodes.csv",
        nodes_column="node",
        node_types_separator="|",
        node_list_node_types_column="node_class_ids",
        node_list_numeric_node_type_ids=True,
        edge_type_path="kg_data/directus/edge_classes.csv",
        edge_path="kg_data/directus/edges.csv",
        sources_column="src_id",
        destinations_column="dst_id",
        edge_list_edge_types_column="edge_class_id",
        edge_list_numeric_node_ids=True,
        edge_list_numeric_edge_type_ids=True,
        name="EMI/Directus KG",
        # Note: These numbers are examples and will vary based on your data
        number_of_nodes=12390676,
        number_of_node_types=56,
        number_of_edge_types=130,
    )

    # Print a summary of the knowledge graph 
    print(kg)

if __name__ == "__main__":
    main()
```
