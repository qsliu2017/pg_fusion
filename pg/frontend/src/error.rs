use thiserror::Error;

#[derive(Debug, Error)]
pub enum PgFrontendError {
    #[error("PostgreSQL Query pointer is null")]
    NullQuery,
    #[error("unsupported PostgreSQL query tree: {0}")]
    Unsupported(String),
    #[error("catalog resolution failed: {0}")]
    Catalog(#[from] df_catalog::ResolveError),
    #[error("DataFusion planning failed: {0}")]
    DataFusion(#[from] datafusion_common::DataFusionError),
    #[error("PostgreSQL scan SQL compilation failed: {0}")]
    ScanSql(#[from] scan_sql::CompileError),
}

impl PgFrontendError {
    pub(crate) fn unsupported(message: impl Into<String>) -> Self {
        Self::Unsupported(message.into())
    }
}
