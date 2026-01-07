//! SQL to Knowledge Graph conversion library.
pub mod edge_class;
pub mod errors;
pub mod node;
pub mod primary_key;
pub mod traits;

/// Prelude module re-exporting commonly used items.
pub mod prelude {
    pub use crate::{edge_class::EdgeClass, node::Node, primary_key::PrimaryKey, traits::KGLikeDB};
}
