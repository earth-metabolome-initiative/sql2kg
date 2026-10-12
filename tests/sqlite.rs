//! End-to-end knowledge graph export from a SQLite database.
#![cfg(feature = "sqlite")]

use diesel::{Connection, RunQueryDsl, SqliteConnection, sql_query};
use sql_traits::prelude::{GenericDialect, ParserDB};
use sql2kg::prelude::*;

const SCHEMA: &str = "
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE tags (label TEXT PRIMARY KEY);
CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    author_id INTEGER REFERENCES users (id),
    tag TEXT REFERENCES tags (label)
);
CREATE TABLE post_tags (
    post_id INTEGER NOT NULL REFERENCES posts (id),
    tag TEXT NOT NULL REFERENCES tags (label),
    PRIMARY KEY (post_id, tag)
);
";

const DATA: &str = "
INSERT INTO users (id, name) VALUES (1, 'ada'), (2, 'grace');
INSERT INTO tags (label) VALUES ('rust'), ('sql');
INSERT INTO posts (id, author_id, tag) VALUES (10, 1, 'rust'), (11, NULL, 'sql');
INSERT INTO post_tags (post_id, tag) VALUES (10, 'rust'), (10, 'sql'), (11, 'sql');
";

fn read(dir: &std::path::Path, file: &str) -> String {
    std::fs::read_to_string(dir.join(file)).expect("CSV must have been written")
}

#[test]
fn exports_sqlite_database() {
    let db = ParserDB::parse::<GenericDialect>(SCHEMA).expect("schema must parse");
    let mut conn = SqliteConnection::establish(":memory:").expect("in-memory database");
    for statement in SCHEMA.split(';').chain(DATA.split(';')).filter(|s| !s.trim().is_empty()) {
        sql_query(statement).execute(&mut conn).expect("statement must run");
    }

    let output = tempfile::tempdir().expect("temporary directory");
    db.write_kg_csvs(&mut conn, output.path()).expect("export must succeed");

    assert_eq!(
        read(output.path(), "nodes.csv"),
        "node,node_class_ids\n\
         \"post_tags(10, rust)\",0\n\
         \"post_tags(10, sql)\",0\n\
         \"post_tags(11, sql)\",0\n\
         \"posts(10)\",1\n\
         \"posts(11)\",1\n\
         \"tags(rust)\",2\n\
         \"tags(sql)\",2\n\
         \"users(1)\",3\n\
         \"users(2)\",3\n"
    );
    assert_eq!(
        read(output.path(), "edge_classes.csv"),
        "edge_class\n\
         \"post_tags(post_id)\"\n\
         \"post_tags(tag)\"\n\
         \"posts(author_id)\"\n\
         \"posts(tag)\"\n"
    );
    // `posts(11)` has a NULL author, so it has no `posts(author_id)` edge.
    assert_eq!(
        read(output.path(), "edges.csv"),
        "src_id,dst_id,edge_class_id\n\
         0,3,0\n\
         1,3,0\n\
         2,4,0\n\
         0,5,1\n\
         1,6,1\n\
         2,6,1\n\
         3,7,2\n\
         3,5,3\n\
         4,6,3\n"
    );
}
