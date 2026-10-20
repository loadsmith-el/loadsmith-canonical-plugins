//! `loadsmith-source-file`: a **raw source** that lists local files for the
//! pipeline's `parser:`.
//!
//! Config: `path` (a directory), `glob` (a file-name pattern, default `*`),
//! optional `recursive` (descend into subdirectories, default `false`). The
//! matching regular files are announced in sorted path order and read **in
//! place**: nothing is copied, no `scratch_dir` is asked for, and the core
//! never deletes them.
//!
//! The pattern matches each file's **name** (not its directory): `*` and `?`
//! never match a leading `.`, so hidden files need an explicit `.*`. A
//! directory that cannot be read is an error; no matching file is not (the
//! parser decides what zero files means).

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use glob::{MatchOptions, Pattern};
use loadsmith_plugin_sdk::ObjectSourcePlugin;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceConfig {
    /// The directory to list.
    path: PathBuf,
    /// File-name pattern.
    #[serde(default = "default_glob")]
    glob: String,
    /// Descend into subdirectories.
    #[serde(default)]
    recursive: bool,
}

fn default_glob() -> String {
    "*".into()
}

/// What `configure` validated.
struct Listing {
    dir: PathBuf,
    pattern: Pattern,
    recursive: bool,
}

const MATCH: MatchOptions = MatchOptions {
    case_sensitive: true,
    require_literal_separator: true,
    require_literal_leading_dot: true,
};

impl Listing {
    fn from_config(config: Value) -> Result<Self> {
        let cfg: SourceConfig = serde_json::from_value(config).context("invalid file source config")?;
        if cfg.glob.contains('/') {
            bail!("glob `{}` matches file names: it cannot contain `/` (use `recursive: true`)", cfg.glob);
        }
        let pattern = Pattern::new(&cfg.glob).with_context(|| format!("invalid glob `{}`", cfg.glob))?;
        let dir = std::path::absolute(&cfg.path)
            .with_context(|| format!("path {} cannot be resolved", cfg.path.display()))?;
        let meta = std::fs::metadata(&dir).with_context(|| format!("path {} is not accessible", dir.display()))?;
        if !meta.is_dir() {
            bail!("path {} is not a directory", dir.display());
        }
        Ok(Self { dir, pattern, recursive: cfg.recursive })
    }

    /// Every matching regular file (symlinks followed), sorted by path.
    fn list(&self) -> Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        let mut dirs = vec![self.dir.clone()];
        while let Some(dir) = dirs.pop() {
            let entries = std::fs::read_dir(&dir).with_context(|| format!("cannot read directory {}", dir.display()))?;
            for entry in entries {
                let entry = entry.with_context(|| format!("cannot read directory {}", dir.display()))?;
                let path = entry.path();
                let meta = match std::fs::metadata(&path) {
                    Ok(m) => m,
                    // A dangling symlink: not a file to parse.
                    Err(_) if entry.file_type().is_ok_and(|t| t.is_symlink()) => continue,
                    Err(e) => return Err(e).with_context(|| format!("cannot stat {}", path.display())),
                };
                if meta.is_dir() {
                    if self.recursive && !is_hidden(&path) {
                        dirs.push(path);
                    }
                } else if meta.is_file() && self.matches(&path) {
                    out.push(path);
                }
            }
        }
        out.sort();
        Ok(out)
    }

    fn matches(&self, path: &Path) -> bool {
        path.file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|name| self.pattern.matches_with(name, MATCH))
    }
}

fn is_hidden(path: &Path) -> bool {
    path.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with('.'))
}

#[derive(Default)]
struct FileSource {
    listing: Option<Listing>,
    /// The files left to announce; `None` until the directory was listed.
    pending: Option<VecDeque<PathBuf>>,
}

#[async_trait]
impl ObjectSourcePlugin for FileSource {
    fn plugin_name(&self) -> &str {
        "loadsmith-source-file"
    }
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        // Plugin config as raw JSON (unknown-key checks: see `PluginConfig::parse`).
        let config = config.into_value();
        self.listing = Some(Listing::from_config(config)?);
        Ok(())
    }

    async fn next_object(&mut self) -> Result<Option<PathBuf>> {
        if self.pending.is_none() {
            let listing = self.listing.as_ref().context("not configured")?;
            let files = listing.list()?;
            if files.is_empty() {
                eprintln!(
                    "no file in {} matches `{}`",
                    listing.dir.display(),
                    listing.pattern.as_str()
                );
            }
            self.pending = Some(files.into());
        }
        Ok(self.pending.as_mut().and_then(VecDeque::pop_front))
    }

    async fn cancel(&mut self) {
        self.pending = Some(VecDeque::new());
    }
}

// mimalloc: release builds are static musl, whose default allocator is slow
// under many small allocations.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_object_source(FileSource::default()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Dir(PathBuf);

    impl Dir {
        fn new(tag: &str) -> Self {
            let p = std::env::temp_dir().join(format!("loadsmith-source-file-{tag}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).unwrap();
            Dir(p)
        }
        fn touch(&self, rel: &str) {
            let p = self.0.join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, b"x").unwrap();
        }
        fn names(&self, files: &[PathBuf]) -> Vec<String> {
            files
                .iter()
                .map(|f| f.strip_prefix(&self.0).unwrap().to_string_lossy().into_owned())
                .collect()
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn listing(config: Value) -> Result<Listing> {
        Listing::from_config(config)
    }

    #[test]
    fn lists_matching_files_sorted_in_place() {
        let d = Dir::new("sorted");
        for f in ["b.csv", "a.csv", "c.txt", "10.csv", ".hidden.csv", "sub/d.csv"] {
            d.touch(f);
        }
        let files = listing(json!({"path": d.0, "glob": "*.csv"})).unwrap().list().unwrap();
        assert_eq!(d.names(&files), ["10.csv", "a.csv", "b.csv"]);
        assert!(files.iter().all(|f| f.is_absolute() && f.starts_with(&d.0)), "paths are in place");
    }

    #[test]
    fn the_default_glob_matches_every_visible_file() {
        let d = Dir::new("default");
        for f in ["a.csv", "b.json", ".dot"] {
            d.touch(f);
        }
        let files = listing(json!({"path": d.0})).unwrap().list().unwrap();
        assert_eq!(d.names(&files), ["a.csv", "b.json"]);
        let hidden = listing(json!({"path": d.0, "glob": ".*"})).unwrap().list().unwrap();
        assert_eq!(d.names(&hidden), [".dot"]);
    }

    #[test]
    fn recursive_descends_into_subdirectories() {
        let d = Dir::new("recursive");
        for f in ["z.csv", "2026/01/a.csv", "2026/02/b.csv", "2026/notes.txt", ".git/x.csv"] {
            d.touch(f);
        }
        let files = listing(json!({"path": d.0, "glob": "*.csv", "recursive": true})).unwrap().list().unwrap();
        assert_eq!(d.names(&files), ["2026/01/a.csv", "2026/02/b.csv", "z.csv"]);
    }

    #[test]
    fn an_empty_match_is_not_an_error() {
        let d = Dir::new("empty");
        d.touch("a.txt");
        assert!(listing(json!({"path": d.0, "glob": "*.csv"})).unwrap().list().unwrap().is_empty());
    }

    #[tokio::test]
    async fn next_object_returns_each_file_once_then_none() {
        let d = Dir::new("next");
        d.touch("b.csv");
        d.touch("a.csv");
        let mut src = FileSource::default();
        src.configure(loadsmith_plugin_sdk::PluginConfig::new(json!({"path": d.0, "glob": "*.csv"}), loadsmith_plugin_sdk::UnknownKeys::Error)).await.unwrap();
        assert_eq!(src.next_object().await.unwrap(), Some(d.0.join("a.csv")));
        assert_eq!(src.next_object().await.unwrap(), Some(d.0.join("b.csv")));
        assert_eq!(src.next_object().await.unwrap(), None);
        assert_eq!(src.next_object().await.unwrap(), None);
    }

    #[test]
    fn bad_configs_are_rejected() {
        let d = Dir::new("bad");
        d.touch("f.csv");
        let err = |v: Value| listing(v).err().map(|e| format!("{e:#}")).unwrap_or_default();
        assert!(err(json!({"glob": "*"})).contains("path"));
        assert!(err(json!({"path": d.0.join("missing")})).contains("not accessible"));
        assert!(err(json!({"path": d.0.join("f.csv")})).contains("not a directory"));
        assert!(err(json!({"path": d.0, "glob": "[a"})).contains("invalid glob"));
        assert!(err(json!({"path": d.0, "glob": "sub/*.csv"})).contains("cannot contain `/`"));
        assert!(err(json!({"path": d.0, "extra": 1})).contains("unknown field"));
    }

    #[test]
    fn an_unreadable_directory_is_an_error() {
        use std::os::unix::fs::PermissionsExt;
        let d = Dir::new("unreadable");
        d.touch("locked/a.csv");
        let locked = d.0.join("locked");
        let l = listing(json!({"path": locked})).unwrap();
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
        let result = l.list();
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
        // Root reads any directory; the check only means something otherwise.
        if std::fs::read_dir("/root").is_err() {
            assert!(format!("{:#}", result.unwrap_err()).contains("cannot read directory"));
        }
    }
}
