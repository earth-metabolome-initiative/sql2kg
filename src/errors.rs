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
    /// A CSV error occurred.
    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),
}
