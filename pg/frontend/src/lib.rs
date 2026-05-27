//! Typed PostgreSQL `Query` tree frontend for DataFusion logical planning.
//!
//! This crate is the first step away from SQL-text planning at the
//! PostgreSQL/DataFusion boundary. It copies PostgreSQL's analyzed type
//! metadata into a stable Rust IR, then compiles the supported subset into a
//! DataFusion logical plan with `PgScanNode` leaves.
//!
//! The current version is intentionally narrow and fail-closed. It is not wired
//! into the production planner hook by default.

mod adapter;
mod compiler;
mod error;
mod ir;
mod operator;
pub mod shippability;

pub use compiler::CompiledQuery;
pub use error::PgFrontendError;
pub use ir::{
    PgBoolOp, PgColumnRef, PgCommand, PgConst, PgConstValue, PgExpr, PgFromItem, PgOperator,
    PgParam, PgParamKind, PgQuery, PgRelationRef, PgTarget, PgTypeRef, PgVar,
};

use df_catalog::{CatalogResolver, PgrxCatalogResolver};
use pgrx::pg_sys;
use scan_node::PgScanSpec;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgFrontendConfig {
    pub identifier_max_bytes: usize,
    pub first_scan_id: u64,
}

impl Default for PgFrontendConfig {
    fn default() -> Self {
        Self {
            identifier_max_bytes: pg_identifier_max_bytes(),
            first_scan_id: 1,
        }
    }
}

#[derive(Debug)]
pub struct PgFrontend<R = PgrxCatalogResolver> {
    resolver: R,
    config: PgFrontendConfig,
}

impl PgFrontend<PgrxCatalogResolver> {
    pub fn new() -> Self {
        Self::with_resolver(PgrxCatalogResolver::new())
    }
}

impl Default for PgFrontend<PgrxCatalogResolver> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> PgFrontend<R> {
    pub fn with_resolver(resolver: R) -> Self {
        Self {
            resolver,
            config: PgFrontendConfig::default(),
        }
    }

    pub fn with_config(mut self, config: PgFrontendConfig) -> Self {
        self.config = config;
        self
    }
}

impl<R> PgFrontend<R>
where
    R: CatalogResolver + Send + Sync,
{
    /// Build a DataFusion logical plan from a PostgreSQL analyzed `Query`.
    ///
    /// # Safety
    ///
    /// `query` must point to a live PostgreSQL analyzed `Query` allocated in a
    /// PostgreSQL memory context that remains valid for the duration of this
    /// call.
    pub unsafe fn build(
        &self,
        query: *mut pg_sys::Query,
    ) -> Result<PgFrontendOutput, PgFrontendError> {
        let pg_query = unsafe { adapter::read_query(query) }?;
        let result = compiler::compile_query(
            pg_query.clone(),
            &self.resolver,
            compiler::CompileConfig {
                identifier_max_bytes: self.config.identifier_max_bytes,
                first_scan_id: self.config.first_scan_id,
            },
        )?;
        let result_targets = pg_query
            .targets
            .into_iter()
            .filter(|target| !target.resjunk)
            .collect();
        Ok(PgFrontendOutput {
            logical_plan: result.logical_plan,
            scans: result.scans,
            result_targets,
            diagnostics: Vec::new(),
        })
    }
}

#[derive(Debug)]
pub struct PgFrontendOutput {
    pub logical_plan: datafusion_expr::logical_plan::LogicalPlan,
    pub scans: Vec<Arc<PgScanSpec>>,
    pub result_targets: Vec<PgTarget>,
    pub diagnostics: Vec<String>,
}

fn pg_identifier_max_bytes() -> usize {
    (pg_sys::NAMEDATALEN as usize).saturating_sub(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_uses_postgres_identifier_limit() {
        assert_eq!(
            PgFrontendConfig::default().identifier_max_bytes,
            (pg_sys::NAMEDATALEN as usize) - 1
        );
    }
}
