//! Typed PostgreSQL `Query` tree frontend for pg_fusion planning.
//!
//! `pg_frontend` turns a live PostgreSQL analyzed [`pg_sys::Query`] into a
//! DataFusion logical plan while preserving the PostgreSQL type and catalog
//! metadata that matters at the scan boundary. It is a planning-time frontend,
//! not a payload format: [`TypedQuery`] values are built, resolved, and compiled
//! in memory, while PostgreSQL `CustomScan` nodes store the already built hybrid
//! plan payload.
//!
//! # Pipeline
//!
//! - `adapter/` copies the supported PostgreSQL tree shape into [`TypedQuery`].
//! - [`resolve_catalog`] mutates the same typed model in place and returns a
//!   [`ResolvedQuery`] view.
//! - The compiler lowers [`ResolvedQuery`] to a DataFusion
//!   [`LogicalPlan`](datafusion_expr::logical_plan::LogicalPlan) with
//!   PostgreSQL planning table-source leaves.
//! - The extension/backend host sends that logical plan through `plan_builder`,
//!   which creates the paired DataFusion and PostgreSQL scan plan.
//!
//! The v1 surface is intentionally narrow and fail-closed. Unsupported
//! PostgreSQL query shapes return structured [`PgFrontendError`] values so the
//! production planner can fall back to SQL-text planning when configured to do
//! so.

mod adapter;
mod compiler;
mod error;
mod operator;
mod resolve;
pub mod shippability;
mod typed_query;

pub use compiler::CompiledQuery;
pub use error::PgFrontendError;
pub use resolve::{resolve_catalog, ResolvedQuery};
pub use typed_query::{
    BoolOp, ColumnRef, Const, FromItem, Param, ParamKind, PgConstValue, PgTypeRef, QueryCommand,
    QueryExpr, QueryOperator, RelationRef, Target, TypedQuery, Var,
};

use df_catalog::{CatalogResolver, PgrxCatalogResolver};
use pgrx::pg_sys;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgFrontendConfig {
    pub identifier_max_bytes: usize,
}

impl Default for PgFrontendConfig {
    fn default() -> Self {
        Self {
            identifier_max_bytes: pg_identifier_max_bytes(),
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
    /// Copy PostgreSQL's analyzed `Query` tree into the stable typed model.
    ///
    /// # Safety
    ///
    /// `query` must point to a live PostgreSQL analyzed `Query` allocated in a
    /// PostgreSQL memory context that remains valid for the duration of this
    /// call.
    pub unsafe fn read_query(
        &self,
        query: *mut pg_sys::Query,
    ) -> Result<TypedQuery, PgFrontendError> {
        unsafe { adapter::read_query(query) }
    }

    /// Build a DataFusion logical plan from a stable typed query model.
    pub fn build_query(&self, mut query: TypedQuery) -> Result<PgFrontendOutput, PgFrontendError> {
        let result_targets = query
            .targets
            .iter()
            .filter(|target| !target.resjunk)
            .cloned()
            .collect();
        let resolved = query.resolve_catalog(&self.resolver)?;
        let result = compiler::compile_query(
            resolved,
            compiler::CompileConfig {
                identifier_max_bytes: self.config.identifier_max_bytes,
            },
        )?;
        Ok(PgFrontendOutput {
            logical_plan: result.logical_plan,
            result_targets,
            diagnostics: Vec::new(),
        })
    }

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
        let typed_query = unsafe { self.read_query(query) }?;
        self.build_query(typed_query)
    }
}

#[derive(Debug)]
pub struct PgFrontendOutput {
    pub logical_plan: datafusion_expr::logical_plan::LogicalPlan,
    pub result_targets: Vec<Target>,
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
