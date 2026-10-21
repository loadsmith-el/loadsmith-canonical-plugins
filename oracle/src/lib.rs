//! Loadsmith Oracle connector (source + destination) on Oracle's official
//! pure-Rust thin driver, the `oracledb` crate — no Oracle Client libraries
//! (no Instant Client / ODPI-C), so the same build runs on amd64 and arm64.
//!
//! `oracledb` is **synchronous**; the SDK plugin traits are async. Each async
//! method runs its blocking DB work inside [`tokio::task::block_in_place`] (the
//! plugin binaries use the multi-threaded runtime). `oracledb::Connection` is
//! `Send`, so it lives in the plugin struct across calls.

pub mod conn;
pub mod destination;
pub mod source;
pub mod telemetry;
pub mod types;
pub mod vocab;

pub use destination::OracleDestPlugin;
pub use source::OracleSourcePlugin;
