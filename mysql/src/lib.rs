//! The mysql plugin family — source and destination — sharing one connection
//! layer. Both binaries ([`bin/source.rs`] and [`bin/destination.rs`]) are thin
//! entry points over [`source::MysqlSourcePlugin`] and
//! [`destination::MysqlDestPlugin`].

pub mod conn;
pub mod destination;
pub mod source;
pub mod types;

pub use destination::MysqlDestPlugin;
pub use source::MysqlSourcePlugin;
