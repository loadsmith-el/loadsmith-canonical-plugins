//! `loadsmith-provider-file`: resolves `{{ secret('<instance>', '<ref>') }}`
//! values from local files.
//!
//! A reference is `path[#key]`:
//! - without `#key`, the value is the whole file, trimmed;
//! - with `#key`, the file is parsed as JSON or YAML (by extension: `.json`,
//!   `.yaml`/`.yml`; any other extension tries JSON, then YAML) and `key` (a
//!   dotted path, where a numeric segment indexes an array) selects a scalar.
//!
//! A relative `path` resolves against the optional `base_dir` config key (the
//! working directory without it).
//!
//! **No message produced here ever contains a value** — nor a parser error
//! text, which can quote the document. Errors name the path, the key and at
//! most a line/column.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use loadsmith_plugin_sdk::{ProviderPlugin, ResolveFailure};
use serde::Deserialize;
use serde_json::Value;

const NOT_FOUND: &str = "NOT_FOUND";
const INVALID_REF: &str = "INVALID_REF";
const ACCESS_DENIED: &str = "ACCESS_DENIED";
const RESOLVE_FAILED: &str = "RESOLVE_FAILED";

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    /// Directory that relative reference paths resolve against.
    base_dir: Option<PathBuf>,
}

#[derive(Default)]
struct FileProvider {
    base_dir: Option<PathBuf>,
}

#[async_trait]
impl ProviderPlugin for FileProvider {
    fn plugin_name(&self) -> &str {
        "loadsmith-provider-file"
    }
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        // No `config:` block at all is every default.
        let cfg: FileConfig = if config.is_null() { FileConfig::default() } else { config.parse()? };
        if let Some(dir) = &cfg.base_dir {
            let meta = std::fs::metadata(dir)
                .with_context(|| format!("base_dir {} is not accessible", dir.display()))?;
            if !meta.is_dir() {
                bail!("base_dir {} is not a directory", dir.display());
            }
        }
        self.base_dir = cfg.base_dir;
        Ok(())
    }

    async fn resolve(&mut self, reference: &str) -> Result<String> {
        resolve_ref(self.base_dir.as_deref(), reference).map_err(Into::into)
    }
}

fn fail(code: &str, message: impl Into<String>) -> ResolveFailure {
    ResolveFailure::new(code, message)
}

/// Resolves one `path[#key]` reference.
fn resolve_ref(base_dir: Option<&Path>, reference: &str) -> Result<String, ResolveFailure> {
    let (path, key) = match reference.split_once('#') {
        Some((path, key)) => (path, Some(key)),
        None => (reference, None),
    };
    if path.is_empty() {
        return Err(fail(INVALID_REF, format!("reference '{reference}' has no path (expected path[#key])")));
    }
    let path = match base_dir {
        Some(base) => base.join(path),
        None => PathBuf::from(path),
    };
    let text = read_file(&path)?;
    match key {
        None => Ok(text.trim().to_string()),
        Some(key) => {
            let segments: Vec<&str> = key.split('.').collect();
            if segments.iter().any(|s| s.is_empty()) {
                return Err(fail(INVALID_REF, format!("reference '{reference}' has an empty key segment")));
            }
            let doc = parse_document(&path, &text)?;
            select(&doc, &segments, &path, key)
        }
    }
}

fn read_file(path: &Path) -> Result<String, ResolveFailure> {
    let shown = path.display();
    let bytes = std::fs::read(path).map_err(|e| match e.kind() {
        ErrorKind::NotFound => fail(NOT_FOUND, format!("{shown} does not exist")),
        ErrorKind::PermissionDenied => fail(ACCESS_DENIED, format!("{shown} is not readable")),
        ErrorKind::IsADirectory => fail(INVALID_REF, format!("{shown} is a directory")),
        // An io::Error describes the failure, never the file's contents.
        _ => fail(RESOLVE_FAILED, format!("cannot read {shown}: {e}")),
    })?;
    String::from_utf8(bytes).map_err(|_| fail(INVALID_REF, format!("{shown} is not valid UTF-8")))
}

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Json,
    Yaml,
    Unknown,
}

fn format_of(path: &Path) -> Format {
    match path.extension().and_then(|e| e.to_str()).map(str::to_ascii_lowercase).as_deref() {
        Some("json") => Format::Json,
        Some("yaml" | "yml") => Format::Yaml,
        _ => Format::Unknown,
    }
}

/// Parses `text` as JSON or YAML. Parser error texts are never passed on: they
/// can quote the document. Only the line and column are reported.
fn parse_document(path: &Path, text: &str) -> Result<Value, ResolveFailure> {
    let shown = path.display();
    let json = || serde_json::from_str::<Value>(text);
    let yaml = || serde_yaml::from_str::<Value>(text);
    match format_of(path) {
        Format::Json => json().map_err(|e| {
            fail(INVALID_REF, format!("{shown} is not valid JSON (line {}, column {})", e.line(), e.column()))
        }),
        Format::Yaml => yaml().map_err(|e| {
            let at = e
                .location()
                .map(|l| format!(" (line {}, column {})", l.line(), l.column()))
                .unwrap_or_default();
            fail(INVALID_REF, format!("{shown} is not valid YAML, or has non-string keys{at}"))
        }),
        Format::Unknown => json()
            .or_else(|_| yaml())
            .map_err(|_| fail(INVALID_REF, format!("{shown} is neither valid JSON nor valid YAML"))),
    }
}

/// Walks the dotted `segments` down `doc` and returns the scalar found there.
fn select(doc: &Value, segments: &[&str], path: &Path, key: &str) -> Result<String, ResolveFailure> {
    let shown = path.display();
    let mut node = doc;
    for segment in segments {
        let next = match node {
            Value::Object(map) => map.get(*segment),
            Value::Array(items) => segment.parse::<usize>().ok().and_then(|i| items.get(i)),
            _ => None,
        };
        node = next.ok_or_else(|| fail(NOT_FOUND, format!("no key '{key}' in {shown}")))?;
    }
    match node {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Null => Err(fail(NOT_FOUND, format!("key '{key}' in {shown} is null"))),
        Value::Object(_) | Value::Array(_) => Err(fail(
            INVALID_REF,
            format!("key '{key}' in {shown} selects an object or a list, not a single value"),
        )),
    }
}

// mimalloc: release builds are static musl, whose default allocator is slow
// under many small allocations.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_provider(FileProvider::default()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "s3cr3t-Value!";

    /// A scratch directory under the system temp dir, removed on drop.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("loadsmith-provider-file-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
        fn write(&self, name: &str, contents: &str) -> PathBuf {
            let path = self.0.join(name);
            std::fs::write(&path, contents).unwrap();
            path
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn err(base: Option<&Path>, reference: &str) -> ResolveFailure {
        let e = resolve_ref(base, reference).expect_err("expected a failure");
        assert!(!e.message.contains(SECRET), "message leaks the value: {}", e.message);
        e
    }

    #[test]
    fn whole_file_is_trimmed() {
        let s = Scratch::new("whole");
        s.write("token", &format!("  {SECRET}\n\n"));
        assert_eq!(resolve_ref(Some(&s.0), "token").unwrap(), SECRET);
    }

    #[test]
    fn absolute_path_ignores_base_dir() {
        let s = Scratch::new("abs");
        let p = s.write("token", SECRET);
        let other = Scratch::new("abs-other");
        assert_eq!(resolve_ref(Some(&other.0), p.to_str().unwrap()).unwrap(), SECRET);
        assert_eq!(resolve_ref(None, p.to_str().unwrap()).unwrap(), SECRET);
    }

    #[test]
    fn json_key_with_dotted_path_and_index() {
        let s = Scratch::new("json");
        s.write(
            "db.json",
            &format!(r#"{{"password": "{SECRET}", "pg": {{"port": 5432, "tls": true, "hosts": ["a", "b"]}}}}"#),
        );
        let base = Some(s.0.as_path());
        assert_eq!(resolve_ref(base, "db.json#password").unwrap(), SECRET);
        assert_eq!(resolve_ref(base, "db.json#pg.port").unwrap(), "5432");
        assert_eq!(resolve_ref(base, "db.json#pg.tls").unwrap(), "true");
        assert_eq!(resolve_ref(base, "db.json#pg.hosts.1").unwrap(), "b");
    }

    #[test]
    fn yaml_key_by_extension() {
        let s = Scratch::new("yaml");
        s.write("db.yaml", &format!("pg:\n  password: \"{SECRET}\"\n  user: loader\n"));
        s.write("db.yml", "user: other\n");
        let base = Some(s.0.as_path());
        assert_eq!(resolve_ref(base, "db.yaml#pg.password").unwrap(), SECRET);
        assert_eq!(resolve_ref(base, "db.yaml#pg.user").unwrap(), "loader");
        assert_eq!(resolve_ref(base, "db.yml#user").unwrap(), "other");
    }

    #[test]
    fn unknown_extension_tries_json_then_yaml() {
        let s = Scratch::new("sniff");
        s.write("a.conf", &format!(r#"{{"k": "{SECRET}"}}"#));
        s.write("b.conf", &format!("k: {SECRET}\n"));
        let base = Some(s.0.as_path());
        assert_eq!(resolve_ref(base, "a.conf#k").unwrap(), SECRET);
        assert_eq!(resolve_ref(base, "b.conf#k").unwrap(), SECRET);
    }

    #[test]
    fn missing_file_is_not_found() {
        let s = Scratch::new("missing");
        assert_eq!(err(Some(&s.0), "nope.json#k").code, NOT_FOUND);
        assert_eq!(err(Some(&s.0), "nope").code, NOT_FOUND);
    }

    #[test]
    fn missing_or_null_key_is_not_found() {
        let s = Scratch::new("nokey");
        s.write("db.json", &format!(r#"{{"password": "{SECRET}", "empty": null, "list": [1]}}"#));
        let base = Some(s.0.as_path());
        assert_eq!(err(base, "db.json#user").code, NOT_FOUND);
        assert_eq!(err(base, "db.json#password.inner").code, NOT_FOUND);
        assert_eq!(err(base, "db.json#empty").code, NOT_FOUND);
        assert_eq!(err(base, "db.json#list.5").code, NOT_FOUND);
    }

    #[test]
    fn malformed_references_are_invalid() {
        let s = Scratch::new("invalid");
        s.write("db.json", &format!(r#"{{"pg": {{"password": "{SECRET}"}}}}"#));
        let base = Some(s.0.as_path());
        assert_eq!(err(base, "").code, INVALID_REF);
        assert_eq!(err(base, "#password").code, INVALID_REF);
        assert_eq!(err(base, "db.json#").code, INVALID_REF);
        assert_eq!(err(base, "db.json#pg..password").code, INVALID_REF);
        // A non-scalar selection.
        assert_eq!(err(base, "db.json#pg").code, INVALID_REF);
        // A directory.
        assert_eq!(err(None, s.0.to_str().unwrap()).code, INVALID_REF);
    }

    #[test]
    fn parse_errors_never_quote_the_document() {
        let s = Scratch::new("parse");
        s.write("bad.json", &format!(r#"{{"password": {SECRET}}}"#));
        s.write("bad.yaml", &format!("password: [{SECRET}\n"));
        s.write("bad.txt", &format!("{{{SECRET}: [\n"));
        let base = Some(s.0.as_path());
        for reference in ["bad.json#password", "bad.yaml#password", "bad.txt#password"] {
            assert_eq!(err(base, reference).code, INVALID_REF, "{reference}");
        }
    }

    #[test]
    fn non_utf8_is_invalid() {
        let s = Scratch::new("utf8");
        std::fs::write(s.0.join("bin"), [0xff, 0xfe, 0x00]).unwrap();
        assert_eq!(err(Some(&s.0), "bin").code, INVALID_REF);
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_file_is_access_denied() {
        use std::os::unix::fs::PermissionsExt;
        let s = Scratch::new("denied");
        let p = s.write("locked", SECRET);
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o000)).unwrap();
        // root reads anything; the check only means something for other users.
        if std::fs::read(&p).is_err() {
            assert_eq!(err(Some(&s.0), "locked").code, ACCESS_DENIED);
        }
    }

    #[tokio::test]
    async fn configure_accepts_no_config_and_validates_base_dir() {
        let s = Scratch::new("configure");
        let mut p = FileProvider::default();
        p.configure(loadsmith_plugin_sdk::PluginConfig::new(Value::Null, loadsmith_plugin_sdk::UnknownKeys::Error)).await.unwrap();
        p.configure(loadsmith_plugin_sdk::PluginConfig::new(serde_json::json!({}), loadsmith_plugin_sdk::UnknownKeys::Error)).await.unwrap();
        assert!(p.base_dir.is_none());

        p.configure(loadsmith_plugin_sdk::PluginConfig::new(serde_json::json!({ "base_dir": s.0 }), loadsmith_plugin_sdk::UnknownKeys::Error)).await.unwrap();
        assert_eq!(p.base_dir.as_deref(), Some(s.0.as_path()));

        let file = s.write("f", "x");
        assert!(p.configure(loadsmith_plugin_sdk::PluginConfig::new(serde_json::json!({ "base_dir": file }), loadsmith_plugin_sdk::UnknownKeys::Error)).await.is_err());
        assert!(p.configure(loadsmith_plugin_sdk::PluginConfig::new(serde_json::json!({ "base_dir": s.0.join("nope") }), loadsmith_plugin_sdk::UnknownKeys::Error)).await.is_err());
        let e = p.configure(loadsmith_plugin_sdk::PluginConfig::new(serde_json::json!({ "uri": "file:///x" }), loadsmith_plugin_sdk::UnknownKeys::Error)).await.unwrap_err();
        assert!(format!("{e:#}").contains("unknown config key `uri`"), "{e:#}");
        // Under `--ignore-unknown-keys` the key is only warned about.
        p.configure(loadsmith_plugin_sdk::PluginConfig::new(serde_json::json!({ "uri": "file:///x" }), loadsmith_plugin_sdk::UnknownKeys::Warn)).await.unwrap();
    }

    #[tokio::test]
    async fn resolve_maps_failures_to_resolve_failure() {
        let s = Scratch::new("trait");
        s.write("token", SECRET);
        let mut p = FileProvider::default();
        p.configure(loadsmith_plugin_sdk::PluginConfig::new(serde_json::json!({ "base_dir": s.0 }), loadsmith_plugin_sdk::UnknownKeys::Error)).await.unwrap();
        assert_eq!(p.resolve("token").await.unwrap(), SECRET);
        let e = p.resolve("absent").await.unwrap_err();
        let failure = e.downcast_ref::<ResolveFailure>().expect("a ResolveFailure");
        assert_eq!(failure.code, NOT_FOUND);
    }
}
