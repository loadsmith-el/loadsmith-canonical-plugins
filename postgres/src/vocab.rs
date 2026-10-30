//! Enum-valued config options: one kebab-case spelling per value, and a "did
//! you mean" for anything else.
//!
//! Every enum option of the canonical plugins is spelled in kebab-case
//! (`verify-full`, `read-committed`, `on-duplicate-key`). A value that isn't
//! one of them is rejected with the list of valid values and, when one is
//! likely meant, a suggestion: a retired spelling names its replacement
//! (`atomic` → `append`), a snake_case or upper-case spelling names its
//! kebab-case form (`verify_full` → `verify-full`), and a near miss names the
//! closest value (`apend` → `append`).
//!
//! [`config_enum!`] declares such an enum; its `Deserialize` goes through
//! [`choose`], so the rejection surfaces as `INVALID_CONFIG` at the value's
//! path through `PluginConfig::parse`.

use serde::de::{self, Deserialize, Deserializer};

/// Declares a config enum: the option it is the value of (named in errors,
/// since a value inside a flattened block has no path), the variants with
/// their kebab-case spellings, and retired spellings with the value that
/// replaces each one.
macro_rules! config_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident for $option:literal {
            $( $(#[$vmeta:meta])* $variant:ident = $spelling:literal ),+ $(,)?
        }
        retired { $( $old:literal => $new:literal ),* $(,)? }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        $vis enum $name {
            $( $(#[$vmeta])* $variant ),+
        }

        impl $name {
            /// The option this enum is the value of.
            pub const OPTION: &'static str = $option;
            /// Every accepted spelling, with its value.
            pub const VALUES: &'static [(&'static str, $name)] = &[$(($spelling, $name::$variant)),+];
            /// Retired spellings, each with the spelling that replaces it.
            pub const RETIRED: &'static [(&'static str, &'static str)] = &[$(($old, $new)),*];

            /// The config spelling of this value.
            #[allow(dead_code)]
            pub fn as_str(self) -> &'static str {
                match self { $($name::$variant => $spelling),+ }
            }
        }

        // The published schema lists the accepted spellings (not the retired
        // ones: they fail with a hint).
        impl schemars::JsonSchema for $name {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                stringify!($name).into()
            }
            fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
                schemars::json_schema!({ "type": "string", "enum": [$($spelling),+] })
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                $crate::vocab::deserialize_choice(d, Self::OPTION, Self::VALUES, Self::RETIRED)
            }
        }
    };
}
pub(crate) use config_enum;

/// `Deserialize` body of a [`config_enum!`] enum.
pub fn deserialize_choice<'de, D: Deserializer<'de>, T: Copy>(
    d: D,
    option: &str,
    values: &[(&str, T)],
    retired: &[(&str, &str)],
) -> Result<T, D::Error> {
    let s = String::deserialize(d)?;
    choose(option, &s, values, retired).map_err(de::Error::custom)
}

/// Resolves `value` (of `option`) to one of `values`, or explains why it isn't
/// one.
pub fn choose<T: Copy>(
    option: &str,
    value: &str,
    values: &[(&str, T)],
    retired: &[(&str, &str)],
) -> Result<T, String> {
    if let Some((_, v)) = values.iter().find(|(name, _)| *name == value) {
        return Ok(*v);
    }
    let expected = values.iter().map(|(n, _)| format!("`{n}`")).collect::<Vec<_>>().join(", ");
    let hint = match suggestion(value, values, retired) {
        Some(s) => format!(" (did you mean `{s}`?)"),
        None => String::new(),
    };
    Err(format!("unknown `{option}` value `{value}`{hint}; expected one of {expected}"))
}

/// The value the user most likely meant, if any.
fn suggestion<T>(value: &str, values: &[(&str, T)], retired: &[(&str, &str)]) -> Option<String> {
    let norm = normalize(value);
    if let Some((_, new)) = retired.iter().find(|(old, _)| normalize(old) == norm) {
        return Some((*new).to_string());
    }
    if let Some((name, _)) = values.iter().find(|(name, _)| *name == norm) {
        return Some((*name).to_string());
    }
    values
        .iter()
        .map(|(name, _)| (levenshtein(&norm, name), *name))
        .filter(|(d, name)| *d <= 2 && *d < name.len())
        .min_by_key(|(d, _)| *d)
        .map(|(_, name)| name.to_string())
}

/// Lower-cased, with `_` and spaces turned into `-` (the kebab-case form).
fn normalize(s: &str) -> String {
    s.trim().to_ascii_lowercase().replace(['_', ' '], "-")
}

fn levenshtein(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut cur = vec![i + 1; b.len() + 1];
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != *cb);
            cur[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(cur[j] + 1);
        }
        prev = cur;
    }
    prev[b.len()]
}

// ── Timestamp unit ───────────────────────────────────────────────────────────

config_enum! {
    /// `timestamp_unit`: the Arrow unit a source emits timestamps in. The
    /// default `ms` is the same in every plugin; a finer unit keeps the
    /// database's own precision.
    #[derive(Default)]
    pub enum TimestampUnit for "timestamp_unit" {
        S = "s",
        #[default]
        Ms = "ms",
        Us = "us",
        Ns = "ns",
    }
    retired {
        "second" => "s", "seconds" => "s", "sec" => "s",
        "millisecond" => "ms", "milliseconds" => "ms", "millis" => "ms",
        "microsecond" => "us", "microseconds" => "us", "micros" => "us", "µs" => "us",
        "nanosecond" => "ns", "nanoseconds" => "ns", "nanos" => "ns",
    }
}

impl TimestampUnit {
    /// The Arrow time unit.
    #[allow(dead_code)]
    pub fn arrow(self) -> arrow_schema::TimeUnit {
        match self {
            TimestampUnit::S => arrow_schema::TimeUnit::Second,
            TimestampUnit::Ms => arrow_schema::TimeUnit::Millisecond,
            TimestampUnit::Us => arrow_schema::TimeUnit::Microsecond,
            TimestampUnit::Ns => arrow_schema::TimeUnit::Nanosecond,
        }
    }
}

/// `dt` (a UTC wall clock) as an epoch value in `unit`, floored to it. `ns`
/// covers the years 1677–2262 only; a value outside fails with a hint.
#[allow(dead_code)]
pub fn epoch_in(dt: chrono::NaiveDateTime, unit: arrow_schema::TimeUnit) -> Result<i64, String> {
    use arrow_schema::TimeUnit::*;
    let utc = dt.and_utc();
    Ok(match unit {
        Second => utc.timestamp(),
        Millisecond => utc.timestamp_millis(),
        Microsecond => utc.timestamp_micros(),
        Nanosecond => utc.timestamp_nanos_opt().ok_or_else(|| {
            format!("{dt} is outside the nanosecond range (1677-2262); use `timestamp_unit: us`")
        })?,
    })
}

/// An epoch value in `unit` back to a UTC wall clock.
#[allow(dead_code)]
pub fn from_epoch(v: i64, unit: arrow_schema::TimeUnit) -> Option<chrono::NaiveDateTime> {
    use arrow_schema::TimeUnit::*;
    let dt = match unit {
        Second => chrono::DateTime::from_timestamp(v, 0),
        Millisecond => chrono::DateTime::from_timestamp_millis(v),
        Microsecond => chrono::DateTime::from_timestamp_micros(v),
        Nanosecond => Some(chrono::DateTime::from_timestamp_nanos(v)),
    };
    dt.map(|d| d.naive_utc())
}

/// A time-of-day value in `unit` (seconds..nanoseconds since midnight).
#[allow(dead_code)]
pub fn time_of_day(v: i64, unit: arrow_schema::TimeUnit) -> Option<chrono::NaiveTime> {
    use arrow_schema::TimeUnit::*;
    let (secs, nanos) = match unit {
        Second => (v, 0),
        Millisecond => (v.div_euclid(1_000), v.rem_euclid(1_000) * 1_000_000),
        Microsecond => (v.div_euclid(1_000_000), v.rem_euclid(1_000_000) * 1_000),
        Nanosecond => (v.div_euclid(1_000_000_000), v.rem_euclid(1_000_000_000)),
    };
    chrono::NaiveTime::from_num_seconds_from_midnight_opt(u32::try_from(secs).ok()?, nanos as u32)
}

// ── The shared `tls` block ───────────────────────────────────────────────────

config_enum! {
    /// `tls.mode`: the TLS posture (libpq's `sslmode` vocabulary).
    #[derive(Default)]
    pub enum TlsMode for "tls.mode" {
        #[default]
        Disable = "disable",
        Prefer = "prefer",
        Require = "require",
        VerifyCa = "verify-ca",
        VerifyFull = "verify-full",
    }
    retired {}
}

/// The `tls` block every database connector takes. The same shape as
/// `loadsmith_tls::TlsConfig`, parsed here so `mode` gets the "did you mean"
/// treatment of the other enum options.
#[derive(Debug, Clone, Default, serde::Deserialize, schemars::JsonSchema)]
pub struct TlsBlock {
    #[serde(default)]
    pub mode: TlsMode,
    /// Trusted CA certificate(s), inline PEM.
    #[serde(default)]
    pub root_cert: Option<String>,
    /// mTLS client certificate, inline PEM.
    #[serde(default)]
    pub client_cert: Option<String>,
    /// mTLS client key, inline PEM.
    #[serde(default)]
    pub client_key: Option<String>,
}

impl TlsBlock {
    /// The engine's generic TLS config for this block.
    #[allow(dead_code)]
    pub fn to_tls_config(&self) -> loadsmith_tls::TlsConfig {
        loadsmith_tls::TlsConfig {
            mode: match self.mode {
                TlsMode::Disable => loadsmith_tls::TlsMode::Disable,
                TlsMode::Prefer => loadsmith_tls::TlsMode::Prefer,
                TlsMode::Require => loadsmith_tls::TlsMode::Require,
                TlsMode::VerifyCa => loadsmith_tls::TlsMode::VerifyCa,
                TlsMode::VerifyFull => loadsmith_tls::TlsMode::VerifyFull,
            },
            root_cert: self.root_cert.clone(),
            client_cert: self.client_cert.clone(),
            client_key: self.client_key.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_units() {
        let u: TimestampUnit = serde_json::from_value(serde_json::json!("us")).unwrap();
        assert_eq!(u.arrow(), arrow_schema::TimeUnit::Microsecond);
        let e = serde_json::from_value::<TimestampUnit>(serde_json::json!("microseconds")).unwrap_err().to_string();
        assert!(e.contains("did you mean `us`?"), "{e}");
        let dt = chrono::NaiveDate::from_ymd_opt(1969, 12, 31).unwrap().and_hms_micro_opt(23, 59, 59, 999_999).unwrap();
        assert_eq!(epoch_in(dt, arrow_schema::TimeUnit::Millisecond).unwrap(), -1);
        assert_eq!(epoch_in(dt, arrow_schema::TimeUnit::Microsecond).unwrap(), -1);
        assert_eq!(epoch_in(dt, arrow_schema::TimeUnit::Second).unwrap(), -1);
        let far = chrono::NaiveDate::from_ymd_opt(3000, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        assert!(epoch_in(far, arrow_schema::TimeUnit::Nanosecond).unwrap_err().contains("timestamp_unit: us"));
        assert_eq!(from_epoch(-1, arrow_schema::TimeUnit::Microsecond), Some(dt));
        assert_eq!(
            time_of_day(3_723_000_001, arrow_schema::TimeUnit::Nanosecond).unwrap().to_string(),
            "00:00:03.723000001"
        );
    }

    #[test]
    fn tls_mode_suggests_the_kebab_case_spelling() {
        let e = serde_json::from_value::<TlsBlock>(serde_json::json!({ "mode": "verify_full" }))
            .unwrap_err()
            .to_string();
        assert!(e.contains("did you mean `verify-full`?"), "{e}");
        let tls: TlsBlock = serde_json::from_value(serde_json::json!({ "mode": "verify-ca" })).unwrap();
        assert_eq!(tls.to_tls_config().mode, loadsmith_tls::TlsMode::VerifyCa);
    }

    config_enum! {
        enum Mode for "mode" {
            Append = "append",
            Overwrite = "overwrite",
            Merge = "merge",
        }
        retired { "atomic" => "append", "staged_merge" => "merge" }
    }

    fn err(value: &str) -> String {
        choose("mode", value, Mode::VALUES, Mode::RETIRED).unwrap_err()
    }

    #[test]
    fn accepts_the_kebab_case_spellings() {
        assert_eq!(choose("mode", "merge", Mode::VALUES, Mode::RETIRED), Ok(Mode::Merge));
        assert_eq!(Mode::Overwrite.as_str(), "overwrite");
    }

    #[test]
    fn retired_spellings_name_their_replacement() {
        let e = err("atomic");
        assert!(e.contains("did you mean `append`?"), "{e}");
        assert!(e.contains("expected one of `append`, `overwrite`, `merge`"), "{e}");
        assert!(err("staged_merge").contains("did you mean `merge`?"));
        assert!(err("staged-merge").contains("did you mean `merge`?"));
    }

    #[test]
    fn snake_case_upper_case_and_typos_are_suggested() {
        assert!(err("MERGE").contains("did you mean `merge`?"));
        assert!(err("apend").contains("did you mean `append`?"));
        assert!(!err("something-else").contains("did you mean"));
    }

    #[test]
    fn deserializes_through_serde() {
        let m: Mode = serde_json::from_value(serde_json::json!("overwrite")).unwrap();
        assert_eq!(m, Mode::Overwrite);
        let e = serde_json::from_value::<Mode>(serde_json::json!("atomic")).unwrap_err().to_string();
        assert!(e.contains("did you mean `append`?"), "{e}");
    }
}
