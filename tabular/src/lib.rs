//! Shared CSV/Excel parsing for the tabular plugins.
//!
//! - [`table`]: a fully-materialized cell table and its type inference (the
//!   `sharepoint` source and the `excel` parser);
//! - [`csv`] / [`excel`]: file bytes (or a workbook path) → [`table::Table`];
//! - [`typed`]: text cells → typed Arrow batches, the declared `columns:`
//!   option and text-value inference (the `csv` and `excel` parsers).
//!
//! A library only: no plugin, no binary, never released on its own.

pub mod csv;
pub mod excel;
pub mod table;
pub mod typed;
