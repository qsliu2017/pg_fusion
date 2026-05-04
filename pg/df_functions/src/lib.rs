//! PostgreSQL-compatible DataFusion function definitions.

mod pg_avg;

pub use pg_avg::{pg_avg_udaf, PgAvg};
