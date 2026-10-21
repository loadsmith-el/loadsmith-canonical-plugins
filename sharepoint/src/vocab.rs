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
//! path through `PluginConfig::parse`. The `kind` tags of the internally
//! tagged config enums are serde's own; [`hint_unknown_variant`] adds the same
//! "did you mean" to serde's "unknown variant" message.

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

/// Adds a "did you mean" to serde's ``unknown variant `x`, expected one of
/// `a`, `b` `` rejection (a `kind` tag), e.g. a snake_case spelling of a
/// kebab-case variant. Any other rejection is returned as is.
pub fn hint_unknown_variant(
    rejection: loadsmith_plugin_sdk::ConfigRejection,
) -> loadsmith_plugin_sdk::ConfigRejection {
    use loadsmith_plugin_sdk::ConfigRejection;
    let ConfigRejection::Invalid { path, message } = rejection else {
        return rejection;
    };
    let hint = message.split_once("unknown variant `").and_then(|(_, rest)| {
        let (value, rest) = rest.split_once('`')?;
        let (_, expected) = rest.split_once("expected")?;
        let names: Vec<(&str, ())> = expected.split('`').skip(1).step_by(2).map(|n| (n, ())).collect();
        suggestion(value, &names, &[])
    });
    let message = match hint {
        Some(s) => format!("{message} (did you mean `{s}`?)"),
        None => message,
    };
    ConfigRejection::Invalid { path, message }
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


#[cfg(test)]
mod tests {
    use super::*;

    config_enum! {
        enum Mode for "mode" {
            Append = "append",
            Overwrite = "overwrite",
        }
        retired { "atomic" => "append" }
    }

    #[test]
    fn suggests_retired_snake_case_and_near_spellings() {
        let err = |v: &str| choose("mode", v, Mode::VALUES, Mode::RETIRED).unwrap_err();
        assert!(err("atomic").contains("did you mean `append`?"));
        assert!(err("OVERWRITE").contains("did you mean `overwrite`?"));
        assert!(err("apend").contains("did you mean `append`?"));
        assert!(!err("something-else").contains("did you mean"));
        assert_eq!(choose("mode", "append", Mode::VALUES, Mode::RETIRED), Ok(Mode::Append));
        assert_eq!(Mode::Overwrite.as_str(), "overwrite");
    }

    #[test]
    fn hints_serde_unknown_variants() {
        use loadsmith_plugin_sdk::ConfigRejection;
        let rej = ConfigRejection::Invalid {
            path: Some("transform[0]".into()),
            message: "unknown variant `drop_empty_rows`, expected one of `filter`, `drop-empty-rows`".into(),
        };
        let ConfigRejection::Invalid { message, .. } = hint_unknown_variant(rej) else { panic!() };
        assert!(message.ends_with("(did you mean `drop-empty-rows`?)"), "{message}");
        let rej = ConfigRejection::Invalid { path: None, message: "unknown variant `zzz`, expected `file` or `list`".into() };
        let ConfigRejection::Invalid { message, .. } = hint_unknown_variant(rej) else { panic!() };
        assert!(!message.contains("did you mean"), "{message}");
    }
}
