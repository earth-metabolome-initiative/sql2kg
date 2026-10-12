//! Traits submodule defining the `SQL2KG` trait for SQL to Knowledge Graph
//! conversion.

pub mod kg_connection;
pub mod kg_like_db;
pub use kg_connection::KgConnection;
pub use kg_like_db::KGLikeDB;
