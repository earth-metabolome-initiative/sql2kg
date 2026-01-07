//! Errors that can occur during SQL to Knowledge Graph conversion.

#[derive(Debug, thiserror::Error)]
/// Enum representing errors that can occur in the SQL to Knowledge Graph
/// conversion process.
pub enum Error {
    /// A diesel error occurred.
    #[error("Diesel error: {0}")]
    Diesel(#[from] diesel::result::Error),
}
