//! Errors that can occur during SQL to Knowledge Graph conversion.

#[derive(Debug, thiserror::Error)]
/// Enum representing errors that can occur in the SQL to Knowledge Graph
/// conversion process.
pub enum Error {
    /// A diesel error occurred.
    #[error("Diesel error: {0}")]
    Diesel(#[from] diesel::result::Error),
    /// An IO error occurred.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// A node from the edge list could not be found in the node list.
    #[error("Node not found: {0}")]
    NodeNotFound(String),
    /// An edge class from the edge list could not be found in the edge class
    /// list.
    #[error("Edge class not found: {0}")]
    EdgeClassNotFound(String),
}
