//! The postgres plugin family — source and destination — sharing one connection
//! layer. Both binaries ([`bin/source.rs`] and [`bin/destination.rs`]) are thin
//! entry points over [`source::PostgresSourcePlugin`] and
//! [`destination::PostgresDestPlugin`].

pub mod conn;
pub mod copy;
pub mod destination;
pub mod source;
pub mod types;

pub use destination::PostgresDestPlugin;
pub use source::PostgresSourcePlugin;
