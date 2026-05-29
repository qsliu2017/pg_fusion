# pg_type

Shared PostgreSQL type policy for pg_fusion.

This crate centralizes the supported PostgreSQL type surface and its
Arrow/DataFusion transport representation. It intentionally does not read or write
PostgreSQL `Datum` values; PostgreSQL-bound crates such as `slot_encoder` and
`slot_import` keep ownership of memory-context, varlena, TOAST, and slot
projection mechanics.
