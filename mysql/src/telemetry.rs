//! Database client telemetry (OpenTelemetry semantic conventions).
//!
//! One `client` span per statement / batch insert / merge phase, plus the
//! `db.client.operation.duration` histogram. `db.query.text` is always the
//! statement the plugin generated (with placeholders) — never a bound value
//! or row data. Nothing is built while telemetry is off.

use std::future::Future;
use std::time::Instant;

use loadsmith_plugin_sdk::telemetry::{self, histogram, kv, KeyValue};
use tracing::field::Empty;
use tracing::{Instrument, Span};

/// `db.system.name` of this plugin.
pub const SYSTEM: &str = "mysql";

/// One database operation being measured.
pub struct DbOp {
    span: Span,
    attrs: Vec<KeyValue>,
}

impl DbOp {
    /// A `client` span named `<operation> <collection>` (or `<operation>`).
    /// `query` is the generated statement text (no values).
    pub fn new(operation: &'static str, namespace: &str, collection: Option<&str>, query: Option<&str>) -> Self {
        if !telemetry::enabled() {
            return Self { span: Span::none(), attrs: Vec::new() };
        }
        let name = match collection {
            Some(c) => format!("{operation} {c}"),
            None => operation.to_string(),
        };
        let span = tracing::info_span!(
            "db",
            otel.name = %name,
            otel.kind = "client",
            db.system.name = SYSTEM,
            db.namespace = namespace,
            db.collection.name = collection,
            db.operation.name = operation,
            db.query.text = query,
            db.response.returned_rows = Empty,
            loadsmith.db.affected_rows = Empty,
            db.operation.batch.size = Empty,
            db.response.status_code = Empty,
            error.type = Empty,
            error = Empty,
        );
        let mut attrs = vec![
            kv("db.system.name", SYSTEM),
            kv("db.namespace", namespace.to_string()),
            kv("db.operation.name", operation),
        ];
        if let Some(c) = collection {
            attrs.push(kv("db.collection.name", c.to_string()));
        }
        Self { span, attrs }
    }

    /// The span, to record more attributes on.
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// Runs `fut` inside the span; records the duration histogram and, on
    /// failure, the error status and `error.type`.
    pub async fn run<T>(mut self, fut: impl Future<Output = anyhow::Result<T>>) -> anyhow::Result<T> {
        if self.span.is_none() {
            return fut.await;
        }
        let started = Instant::now();
        let result = fut.instrument(self.span.clone()).await;
        if let Err(e) = &result {
            let (ty, code) = error_type(e);
            if let Some(code) = &code {
                self.span.record("db.response.status_code", code.as_str());
            }
            self.span.record("error.type", ty.as_str());
            self.span.record("error", format!("{e:#}"));
            self.attrs.push(kv("error.type", ty));
        }
        histogram("db.client.operation.duration").with_unit("s").record(started.elapsed(), &self.attrs);
        result
    }
}

/// `error.type` (and the server error code, when the server sent one) of a
/// failure.
fn error_type(e: &anyhow::Error) -> (String, Option<String>) {
    for cause in e.chain() {
        if let Some(my) = cause.downcast_ref::<mysql_async::Error>() {
            if let mysql_async::Error::Server(server) = my {
                let code = server.code.to_string();
                return (code.clone(), Some(code));
            }
            return ("mysql_async::Error".into(), None);
        }
        if let Some(server) = cause.downcast_ref::<mysql_async::ServerError>() {
            let code = server.code.to_string();
            return (code.clone(), Some(code));
        }
    }
    ("_OTHER".into(), None)
}

/// Runs `fut` as one database operation (see [`DbOp`]).
pub async fn op<T>(
    operation: &'static str,
    namespace: &str,
    collection: Option<&str>,
    query: Option<&str>,
    fut: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    DbOp::new(operation, namespace, collection, query).run(fut).await
}
