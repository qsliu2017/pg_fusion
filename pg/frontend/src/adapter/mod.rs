mod common;

#[cfg(not(feature = "pg17"))]
compile_error!("pg_frontend currently supports only the pg17 feature");

#[cfg(feature = "pg17")]
mod pg17;

#[cfg(feature = "pg17")]
pub(crate) use pg17::read_query;
