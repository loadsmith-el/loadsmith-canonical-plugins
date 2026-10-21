//! SharePoint source plugin for loadsmith.
//!
//! Reads an Excel/CSV file or a SharePoint List from Microsoft Graph, applies
//! in-plugin tabular transforms (the EL engine has no transform stage — these are
//! a connector-internal concern), and emits Arrow. Source-only: SharePoint is
//! read-only here. Ported from the Python `sp-bulk`.

pub mod config;
pub mod graph;
pub mod list;
pub mod parse;
pub mod source;
pub mod table;
pub mod transform;
pub mod vocab;

pub use source::SharepointSourcePlugin;
