//! The postgres plugin family — source and destination — sharing one connection
//! layer. Both binaries ([`bin/source.rs`] and [`bin/destination.rs`]) are thin
//! entry points over [`source::PostgresSourcePlugin`] and
//! [`destination::PostgresDestPlugin`].

pub mod conn;
pub mod copy;
pub mod destination;
pub mod parallel;
pub mod read;
pub mod source;
pub mod telemetry;
pub mod types;
pub mod vocab;

pub use destination::PostgresDestPlugin;
pub use source::PostgresSourcePlugin;
