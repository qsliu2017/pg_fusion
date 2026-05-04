# df_functions

PostgreSQL-compatible DataFusion functions used by `pg_fusion` planning and
worker execution.

The crate keeps compatibility functions separate from PostgreSQL-bound code so
logical planning, physical planning, and tests can register the same UDF/UDAF
definitions.
