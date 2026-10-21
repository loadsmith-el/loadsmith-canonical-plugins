//! Database client telemetry (OpenTelemetry semantic conventions).
//!
//! One `client` span per statement / array insert / merge phase, plus the
//! `db.client.operation.duration` histogram. `db.query.text` is always the
//! statement the plugin generated (with placeholders) — never a bound value
//! or row data. Nothing is built while telemetry is off.

use std::time::Instant;

use loadsmith_plugin_sdk::telemetry::{self, histogram, kv, KeyValue};
use tracing::field::Empty;
use tracing::Span;

/// `db.system.name` of this plugin.
pub const SYSTEM: &str = "oracle.db";

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

    /// Runs the (blocking) driver call `f` inside the span; records the
    /// duration histogram and, on failure, the error status and `error.type`.
    pub fn run<T>(mut self, f: impl FnOnce() -> anyhow::Result<T>) -> anyhow::Result<T> {
        if self.span.is_none() {
            return f();
        }
        let started = Instant::now();
        let result = self.span.in_scope(f);
        if let Err(e) = &result {
            let message = format!("{e:#}");
            let code = ora_code(&message);
            if let Some(code) = &code {
                self.span.record("db.response.status_code", code.as_str());
            }
            let ty = code.unwrap_or_else(|| "_OTHER".into());
            self.span.record("error.type", ty.as_str());
            self.span.record("error", message);
            self.attrs.push(kv("error.type", ty));
        }
        histogram("db.client.operation.duration").with_unit("s").record(started.elapsed(), &self.attrs);
        result
    }
}

/// The first `ORA-NNNNN` code in a driver error message (`error.type`).
fn ora_code(message: &str) -> Option<String> {
    let at = message.find("ORA-")?;
    let digits: String = message[at + 4..].chars().take_while(|c| c.is_ascii_digit()).collect();
    (!digits.is_empty()).then(|| format!("ORA-{digits}"))
}

/// Runs the blocking `f` as one database operation (see [`DbOp`]).
pub fn op<T>(
    operation: &'static str,
    namespace: &str,
    collection: Option<&str>,
    query: Option<&str>,
    f: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    DbOp::new(operation, namespace, collection, query).run(f)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ora_code_is_the_error_type() {
        assert_eq!(ora_code("staged MERGE failed: ORA-30926: unable to get a stable set").as_deref(), Some("ORA-30926"));
        assert_eq!(ora_code("oracle connect failed: timed out"), None);
        assert_eq!(ora_code("ORA-"), None);
    }
}
