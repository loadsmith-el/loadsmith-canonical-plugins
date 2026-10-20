use anyhow::{bail, Result};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow_array::{
    builder::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
        Int32Builder, Int64Builder, StringBuilder,
    },
    ArrayRef, ListArray, RecordBatch, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use postgres_types::Kind;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::types::Type as PgType;
use tokio_postgres::Column;

/// Field metadata key used to mark hstore columns. The value-builder checks this
/// to JSON-encode the Postgres "k"=>"v" text format instead of passing it raw.
const HSTORE_TYPE_KEY: &str = "pg_original_type";
const HSTORE_TYPE_VALUE: &str = "hstore";

static UNIX_EPOCH: std::sync::OnceLock<NaiveDate> = std::sync::OnceLock::new();

fn unix_epoch() -> NaiveDate {
    *UNIX_EPOCH.get_or_init(NaiveDate::default) // 1970-01-01
}

/// Maps a Postgres type to an Arrow DataType.
///
/// - `array` (any element type) → `DataType::List` with a recursively-mapped inner type
/// - `hstore` → `DataType::Utf8` (JSON-encoded; see HSTORE_TYPE_KEY metadata on the Field)
/// - everything else → mapped by type name (NUMERIC/UUID/etc. fall through to Utf8)
pub fn pg_type_to_arrow(pg_type: &PgType, unit: TimeUnit) -> DataType {
    if pg_type.name() == "hstore" {
        return DataType::Utf8;
    }
    if let Kind::Array(inner) = pg_type.kind() {
        let inner_dt = pg_type_to_arrow(inner, unit);
        return DataType::List(Arc::new(Field::new("item", inner_dt, true)));
    }
    match pg_type.name() {
        "int2" | "int4" => DataType::Int32,
        "int8" => DataType::Int64,
        "float4" => DataType::Float32,
        "float8" => DataType::Float64,
        "bool" => DataType::Boolean,
        "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(unit, None),
        // An absolute instant: decoded to UTC, and tagged so downstream (e.g.
        // Parquet) keeps it zone-aware.
        "timestamptz" => DataType::Timestamp(unit, Some("UTC".into())),
        "bytea" => DataType::Binary,
        _ => DataType::Utf8,
    }
}

/// Builds an Arrow Schema from a slice of tokio-postgres columns.
pub fn columns_to_schema(cols: &[Column], unit: TimeUnit) -> Schema {
    let fields: Vec<Field> = cols
        .iter()
        .map(|c| {
            let dt = pg_type_to_arrow(c.type_(), unit);
            if c.type_().name() == "hstore" {
                let meta = HashMap::from([(
                    HSTORE_TYPE_KEY.to_string(),
                    HSTORE_TYPE_VALUE.to_string(),
                )]);
                Field::new(c.name(), dt, true).with_metadata(meta)
            } else {
                Field::new(c.name(), dt, true)
            }
        })
        .collect();
    Schema::new(fields)
}

/// Converts simple_query rows (text protocol) to an Arrow RecordBatch.
pub fn rows_to_batch_text(
    rows: &[tokio_postgres::SimpleQueryRow],
    schema: &Schema,
) -> Result<RecordBatch> {
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| build_column_text(rows, idx, field))
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| anyhow::anyhow!("RecordBatch::try_new: {e}"))
}

fn build_column_text(
    rows: &[tokio_postgres::SimpleQueryRow],
    col_idx: usize,
    field: &Field,
) -> Result<ArrayRef> {
    let values: Vec<Option<&str>> = rows.iter().map(|r| r.get(col_idx)).collect();
    match field.data_type() {
        DataType::List(inner_field) => build_list(&values, inner_field.clone(), field.name()),
        _ => build_values(&values, field, field.name()),
    }
}

/// Builds a `ListArray` column from Postgres text-protocol array notation (`{1,2,3}`).
/// The elements go through [`build_values`] with the list's item field, so arrays
/// decode exactly like scalar columns of the same type.
fn build_list(values: &[Option<&str>], inner_field: Arc<Field>, col_name: &str) -> Result<ArrayRef> {
    let mut lengths: Vec<usize> = Vec::with_capacity(values.len());
    let mut is_valid: Vec<bool> = Vec::with_capacity(values.len());
    let mut items: Vec<Option<String>> = Vec::new();

    for v in values {
        match v {
            None => {
                lengths.push(0);
                is_valid.push(false);
            }
            Some(text) => {
                let parsed = parse_pg_array_text(text)
                    .map_err(|e| anyhow::anyhow!("col {col_name}: array '{text}': {e}"))?;
                lengths.push(parsed.len());
                is_valid.push(true);
                items.extend(parsed);
            }
        }
    }

    let item_refs: Vec<Option<&str>> = items.iter().map(Option::as_deref).collect();
    let child = build_values(&item_refs, &inner_field, &format!("{col_name}[]"))?;
    let offsets = OffsetBuffer::from_lengths(lengths);
    let nulls = Some(NullBuffer::from(is_valid.as_slice()));
    ListArray::try_new(inner_field, offsets, child, nulls)
        .map_err(|e| anyhow::anyhow!("col {col_name}: list build: {e}"))
        .map(|a| Arc::new(a) as ArrayRef)
}

/// Builds a flat Arrow array of `field`'s (scalar) type from Postgres text values.
fn build_values(values: &[Option<&str>], field: &Field, col_name: &str) -> Result<ArrayRef> {
    macro_rules! parsed {
        ($builder:ty, $parse:expr) => {{
            let mut b = <$builder>::with_capacity(values.len());
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(v) => b.append_value(
                        $parse(*v).map_err(|e| anyhow::anyhow!("col {col_name}: '{v}': {e}"))?,
                    ),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }

    Ok(match field.data_type() {
        DataType::Int32 => parsed!(Int32Builder, |v: &str| v.parse::<i32>()),
        DataType::Int64 => parsed!(Int64Builder, |v: &str| v.parse::<i64>()),
        DataType::Float32 => parsed!(Float32Builder, |v: &str| v.parse::<f32>()),
        DataType::Float64 => parsed!(Float64Builder, |v: &str| v.parse::<f64>()),
        DataType::Boolean => parsed!(BooleanBuilder, parse_bool),
        DataType::Date32 => parsed!(Date32Builder, parse_date_days),
        DataType::Timestamp(unit, tz) => {
            let with_offset = tz.is_some();
            let mut out: Vec<Option<i64>> = Vec::with_capacity(values.len());
            for v in values {
                out.push(match v {
                    None => None,
                    Some(v) => Some(
                        parse_timestamp(v, with_offset, *unit)
                            .map_err(|e| anyhow::anyhow!("col {col_name}: '{v}': {e}"))?,
                    ),
                });
            }
            let tz = tz.clone();
            match unit {
                TimeUnit::Second => Arc::new(TimestampSecondArray::from(out).with_timezone_opt(tz)) as ArrayRef,
                TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(out).with_timezone_opt(tz)),
                TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(out).with_timezone_opt(tz)),
                TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(out).with_timezone_opt(tz)),
            }
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::with_capacity(values.len(), values.len() * 16);
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(v) => b.append_value(
                        parse_bytea_hex(v).map_err(|e| anyhow::anyhow!("col {col_name}: bytea: {e}"))?,
                    ),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        DataType::Utf8 => {
            let is_hstore = field.metadata().get(HSTORE_TYPE_KEY).map(String::as_str)
                == Some(HSTORE_TYPE_VALUE);
            let mut b = StringBuilder::with_capacity(values.len(), values.len() * 32);
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(v) if is_hstore => b.append_value(
                        parse_hstore_to_json(v)
                            .map_err(|e| anyhow::anyhow!("col {col_name}: hstore parse: {e}"))?,
                    ),
                    Some(v) => b.append_value(v),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        other => bail!("col {col_name}: unsupported arrow type {other:?}"),
    })
}

fn parse_bool(v: &str) -> Result<bool> {
    match v {
        "t" | "true" => Ok(true),
        "f" | "false" => Ok(false),
        _ => bail!("unexpected bool value"),
    }
}

/// Parses a one-dimensional Postgres array in text form (`{a,"b c",NULL}`)
/// into its elements.
///
/// A quoted element (`"…"`, with `\` escapes) is always a value — `"NULL"` is
/// the string `NULL` — while an unquoted `NULL` is SQL NULL. A non-default
/// lower bound (`[0:2]={…}`) is accepted. A multi-dimensional value
/// (`{{1,2},{3,4}}`) has no single-level Arrow list, so it is rejected with a
/// hint to keep its shape as JSON.
fn parse_pg_array_text(text: &str) -> Result<Vec<Option<String>>> {
    let mut text = text.trim();
    // Explicit bounds: `[1:3]={...}` (or `[1:2][1:2]={...}` for 2-D).
    if text.starts_with('[') {
        let eq = text.find('=').ok_or_else(|| anyhow::anyhow!("malformed array bounds"))?;
        if text[..eq].matches('[').count() > 1 {
            bail!(multi_dim_error());
        }
        text = &text[eq + 1..];
    }
    let inner = text
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| anyhow::anyhow!("not an array literal"))?;

    let mut result = Vec::new();
    let mut chars = inner.chars().peekable();
    if chars.peek().is_none() {
        return Ok(result);
    }
    loop {
        match chars.peek() {
            Some('{') => bail!(multi_dim_error()),
            Some('"') => {
                chars.next();
                let mut value = String::new();
                loop {
                    match chars.next() {
                        None => bail!("unterminated quoted element"),
                        Some('\\') => value.push(chars.next().ok_or_else(|| anyhow::anyhow!("dangling '\\'"))?),
                        Some('"') => break,
                        Some(c) => value.push(c),
                    }
                }
                result.push(Some(value));
            }
            _ => {
                let mut value = String::new();
                while let Some(&c) = chars.peek() {
                    if c == ',' {
                        break;
                    }
                    if c == '"' || c == '{' || c == '}' {
                        bail!("unexpected '{c}' in an unquoted element");
                    }
                    value.push(c);
                    chars.next();
                }
                let value = value.trim();
                result.push(if value.eq_ignore_ascii_case("null") { None } else { Some(value.to_string()) });
            }
        }
        match chars.next() {
            None => return Ok(result),
            Some(',') => {}
            Some(c) => bail!("expected ',' between elements, got '{c}'"),
        }
    }
}

fn multi_dim_error() -> String {
    "multi-dimensional arrays are not supported (an Arrow list has one level); select \
     `array_to_json(col)` to keep the shape as JSON, or `col::text`"
        .to_string()
}

/// Converts the Postgres hstore text format `"k1"=>"v1", "k2"=>NULL` to a JSON string.
fn parse_hstore_to_json(text: &str) -> Result<String> {
    let mut map = serde_json::Map::new();
    let mut chars = text.trim().chars().peekable();

    loop {
        // Skip whitespace
        while chars.peek().map(|c: &char| c.is_whitespace()).unwrap_or(false) {
            chars.next();
        }
        if chars.peek().is_none() {
            break;
        }

        // Key (always double-quoted in hstore)
        if chars.peek() != Some(&'"') {
            bail!("expected '\"' at start of hstore key");
        }
        let key = read_quoted_hstore(&mut chars)?;

        // => separator
        while chars.peek().map(|c: &char| c.is_whitespace()).unwrap_or(false) {
            chars.next();
        }
        let arrow: String = chars.by_ref().take(2).collect();
        if arrow != "=>" {
            bail!("expected '=>' in hstore, got '{arrow}'");
        }
        while chars.peek().map(|c: &char| c.is_whitespace()).unwrap_or(false) {
            chars.next();
        }

        // Value: quoted string or unquoted NULL
        let value = if chars.peek() == Some(&'"') {
            serde_json::Value::String(read_quoted_hstore(&mut chars)?)
        } else {
            let mut v = String::new();
            while let Some(&c) = chars.peek() {
                if c == ',' {
                    break;
                }
                v.push(c);
                chars.next();
            }
            let v = v.trim();
            if v.eq_ignore_ascii_case("null") {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(v.to_string())
            }
        };

        map.insert(key, value);

        // Skip comma
        while chars.peek().map(|c: &char| c.is_whitespace()).unwrap_or(false) {
            chars.next();
        }
        match chars.peek() {
            Some(&',') => {
                chars.next();
            }
            _ => break,
        }
    }

    Ok(serde_json::Value::Object(map).to_string())
}

fn read_quoted_hstore(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Result<String> {
    chars.next(); // consume opening "
    let mut s = String::new();
    loop {
        match chars.next() {
            None => bail!("hstore: unterminated quoted string"),
            Some('\\') => match chars.next() {
                Some(c) => s.push(c),
                None => bail!("hstore: unexpected end after '\\'"),
            },
            Some('"') => break,
            Some(c) => s.push(c),
        }
    }
    Ok(s)
}

/// Rejects `infinity`/`-infinity`, which have no Arrow timestamp/date value.
fn reject_infinity(s: &str) -> Result<()> {
    if s.eq_ignore_ascii_case("infinity") || s.eq_ignore_ascii_case("-infinity") {
        bail!(
            "'{s}' has no Arrow date/timestamp equivalent; map it in the query \
             (e.g. NULLIF(col, '{s}') or a sentinel)"
        );
    }
    Ok(())
}

/// Parses an ISO date (`YYYY-MM-DD`, optionally suffixed ` BC`) into a
/// proleptic-Gregorian `NaiveDate`. Built from its parts so BC leap days (year
/// 1 BC = astronomical year 0 is a leap year) are valid.
fn parse_iso_date(s: &str) -> Result<(NaiveDate, &str)> {
    let (s, bc) = match s.strip_suffix(" BC") {
        Some(rest) => (rest, true),
        None => (s, false),
    };
    let (date, rest) = s.split_at(s.find(' ').unwrap_or(s.len()));
    let mut parts = date.splitn(3, '-');
    let mut next = |what: &str| -> Result<i32> {
        parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing {what}"))?
            .parse::<i32>()
            .map_err(|e| anyhow::anyhow!("bad {what}: {e}"))
    };
    let (y, m, d) = (next("year")?, next("month")?, next("day")?);
    let year = if bc { 1 - y } else { y };
    let date = NaiveDate::from_ymd_opt(year, m as u32, d as u32)
        .ok_or_else(|| anyhow::anyhow!("invalid date"))?;
    Ok((date, rest.trim_start()))
}

/// Parses postgres `date` text (DateStyle ISO) → days since the Unix epoch.
fn parse_date_days(s: &str) -> Result<i32> {
    reject_infinity(s)?;
    let (date, rest) = parse_iso_date(s)?;
    if !rest.is_empty() {
        bail!("unexpected trailing text '{rest}'");
    }
    Ok(date.signed_duration_since(unix_epoch()).num_days() as i32)
}

/// Parses postgres `timestamp` / `timestamptz` text (DateStyle ISO) → an epoch
/// value in `unit` (UTC).
///
/// `timestamp`: `YYYY-MM-DD HH:MM:SS[.ffffff][ BC]`. `timestamptz` additionally
/// carries the session zone's UTC offset right after the time — `+HH`, `+HH:MM`
/// or, for pre-standard-time local mean time, `+HH:MM:SS` — which is subtracted
/// to get the UTC instant. Digits finer than `unit` are truncated (floored).
fn parse_timestamp(s: &str, with_offset: bool, unit: TimeUnit) -> Result<i64> {
    reject_infinity(s)?;
    let (date, time) = parse_iso_date(s)?;
    let (time, offset_secs) = if with_offset {
        let at = time
            .rfind(['+', '-'])
            .ok_or_else(|| anyhow::anyhow!("timestamptz without a UTC offset"))?;
        (&time[..at], parse_utc_offset(&time[at..])?)
    } else {
        (time, 0)
    };
    let time = NaiveTime::parse_from_str(time, "%H:%M:%S%.f").map_err(|e| anyhow::anyhow!("{e}"))?;
    let utc = NaiveDateTime::new(date, time) - chrono::Duration::seconds(offset_secs);
    crate::vocab::epoch_in(utc, unit).map_err(|e| anyhow::anyhow!(e))
}

/// Parses a `+HH[:MM[:SS]]` / `-HH[:MM[:SS]]` UTC offset into seconds.
fn parse_utc_offset(s: &str) -> Result<i64> {
    let (sign, digits) = match s.split_at(1) {
        ("+", rest) => (1, rest),
        ("-", rest) => (-1, rest),
        _ => bail!("bad UTC offset '{s}'"),
    };
    let mut secs = 0i64;
    for (i, part) in digits.split(':').enumerate() {
        if i > 2 {
            bail!("bad UTC offset '{s}'");
        }
        let n: i64 = part.parse().map_err(|_| anyhow::anyhow!("bad UTC offset '{s}'"))?;
        secs += n * [3600, 60, 1][i];
    }
    Ok(sign * secs)
}

/// Parses postgres bytea hex format ("\\xdeadbeef") → bytes.
fn parse_bytea_hex(s: &str) -> Result<Vec<u8>> {
    let hex = s.strip_prefix("\\x").unwrap_or(s).as_bytes();
    if !hex.len().is_multiple_of(2) {
        bail!("odd number of hex digits");
    }
    hex.chunks(2)
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|p| u8::from_str_radix(p, 16).ok())
                .ok_or_else(|| anyhow::anyhow!("invalid hex digits"))
        })
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_pg_array_text ──────────────────────────────────────────────────

    #[test]
    fn array_empty() {
        assert_eq!(parse_pg_array_text("{}").unwrap(), Vec::<Option<String>>::new());
    }

    #[test]
    fn array_quoted_null_is_a_string_and_bounds_are_accepted() {
        let v = parse_pg_array_text(r#"{"NULL",NULL,"a,b","{x}",""}"#).unwrap();
        assert_eq!(v, vec![Some("NULL".into()), None, Some("a,b".into()), Some("{x}".into()), Some("".into())]);
        assert_eq!(parse_pg_array_text("[0:1]={7,8}").unwrap(), vec![Some("7".into()), Some("8".into())]);
    }

    #[test]
    fn array_multi_dimensional_is_a_clear_error() {
        for text in ["{{1,2},{3,4}}", "[1:2][1:1]={{1},{2}}"] {
            let e = parse_pg_array_text(text).unwrap_err().to_string();
            assert!(e.contains("array_to_json"), "{e}");
        }
        assert!(parse_pg_array_text("{\"open").is_err());
        assert!(parse_pg_array_text("nope").is_err());
    }

    #[test]
    fn timestamps_in_every_unit() {
        let s = "2024-01-15 12:30:45.123456+00";
        assert_eq!(parse_timestamp(s, true, TimeUnit::Second).unwrap(), 1_705_321_845);
        assert_eq!(parse_timestamp(s, true, TimeUnit::Microsecond).unwrap(), 1_705_321_845_123_456);
        assert_eq!(parse_timestamp(s, true, TimeUnit::Nanosecond).unwrap(), 1_705_321_845_123_456_000);
        assert!(parse_timestamp("3000-01-01 00:00:00", false, TimeUnit::Nanosecond).is_err());
        let field = Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true);
        let arr = build_values(&[Some(s), None], &field, "ts").unwrap();
        assert_eq!(arr.data_type(), field.data_type());
        assert_eq!(arr.null_count(), 1);
    }

    #[test]
    fn bytea_hex_rejects_bad_input_without_panicking() {
        assert!(parse_bytea_hex("\\xabc").is_err());
        assert!(parse_bytea_hex("\\xzz").is_err());
        assert!(parse_bytea_hex("\\xé1").is_err());
    }

    #[test]
    fn array_integers() {
        let v = parse_pg_array_text("{1,2,3}").unwrap();
        assert_eq!(v, vec![Some("1".into()), Some("2".into()), Some("3".into())]);
    }

    #[test]
    fn array_text_quoted() {
        let v = parse_pg_array_text(r#"{"hello","world"}"#).unwrap();
        assert_eq!(v, vec![Some("hello".into()), Some("world".into())]);
    }

    #[test]
    fn array_text_with_spaces_in_value() {
        let v = parse_pg_array_text(r#"{"hello world","foo bar"}"#).unwrap();
        assert_eq!(v, vec![Some("hello world".into()), Some("foo bar".into())]);
    }

    #[test]
    fn array_with_null_element() {
        let v = parse_pg_array_text("{1,NULL,3}").unwrap();
        assert_eq!(v, vec![Some("1".into()), None, Some("3".into())]);
    }

    #[test]
    fn array_quoted_with_backslash_escape() {
        let v = parse_pg_array_text(r#"{"say \"hi\""}"#).unwrap();
        assert_eq!(v, vec![Some(r#"say "hi""#.into())]);
    }

    // ── parse_hstore_to_json ─────────────────────────────────────────────────

    #[test]
    fn hstore_simple_pair() {
        let json = parse_hstore_to_json(r#""k"=>"v""#).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["k"], "v");
    }

    #[test]
    fn hstore_multiple_pairs() {
        let json = parse_hstore_to_json(r#""a"=>"1", "b"=>"2""#).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["a"], "1");
        assert_eq!(parsed["b"], "2");
    }

    #[test]
    fn hstore_null_value() {
        let json = parse_hstore_to_json(r#""k"=>NULL"#).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed["k"].is_null());
    }

    #[test]
    fn hstore_empty_string_value() {
        let json = parse_hstore_to_json(r#""k"=>"  ""#).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["k"], "  ");
    }

    // ── pg_type_to_arrow ─────────────────────────────────────────────────────

    #[test]
    fn type_int4_array_becomes_list_of_int32() {
        use tokio_postgres::types::Type;
        let dt = pg_type_to_arrow(&Type::INT4_ARRAY, TimeUnit::Millisecond);
        assert!(matches!(dt, DataType::List(_)));
        if let DataType::List(inner) = dt {
            assert_eq!(inner.data_type(), &DataType::Int32);
        }
    }

    #[test]
    fn type_text_becomes_utf8() {
        use tokio_postgres::types::Type;
        let dt = pg_type_to_arrow(&Type::TEXT, TimeUnit::Millisecond);
        assert_eq!(dt, DataType::Utf8);
    }

    #[test]
    fn type_float8_becomes_float64() {
        use tokio_postgres::types::Type;
        let dt = pg_type_to_arrow(&Type::FLOAT8, TimeUnit::Millisecond);
        assert_eq!(dt, DataType::Float64);
    }

    // ── parse_timestamp_millis / parse_bytea_hex (unchanged) ─────────────────

    #[test]
    fn parse_timestamp_millis_basic() {
        let ms = parse_timestamp("2024-01-15 12:30:45.123", false, TimeUnit::Millisecond).unwrap();
        assert_eq!(ms, 1_705_321_845_123);
    }

    #[test]
    fn timestamptz_offsets_normalize_to_utc() {
        let utc = 1_705_321_845_123; // 2024-01-15 12:30:45.123 UTC
        for s in [
            "2024-01-15 12:30:45.123+00",
            "2024-01-15 18:00:45.123+05:30",
            "2024-01-15 09:30:45.123-03",
            "2024-01-15 12:30:45.123456+00", // micros truncated
        ] {
            assert_eq!(parse_timestamp(s, true, TimeUnit::Millisecond).unwrap(), utc, "{s}");
        }
        // Local mean time offsets carry seconds (Asia/Kolkata, year 1).
        assert_eq!(
            parse_timestamp("0001-01-01 05:53:28+05:53:28", true, TimeUnit::Millisecond).unwrap(),
            -62_135_596_800_000
        );
        assert!(parse_timestamp("2024-01-15 12:30:45", true, TimeUnit::Millisecond).is_err());
    }

    #[test]
    fn bc_and_pre_epoch_values() {
        // Values cross-checked against Postgres 15 (extract(epoch …)).
        assert_eq!(parse_date_days("0044-03-15 BC").unwrap(), -735_160);
        assert_eq!(parse_timestamp("0044-03-15 12:00:00 BC", false, TimeUnit::Millisecond).unwrap(), -63_517_780_800_000);
        assert_eq!(parse_timestamp("1969-12-31 23:59:59.999", false, TimeUnit::Millisecond).unwrap(), -1);
        // 1 BC (astronomical year 0) is a leap year.
        assert!(parse_date_days("0001-02-29 BC").is_ok());
    }

    #[test]
    fn infinity_is_a_clear_error() {
        let e = parse_timestamp("infinity", false, TimeUnit::Millisecond).unwrap_err().to_string();
        assert!(e.contains("NULLIF"), "{e}");
        assert!(parse_date_days("-infinity").is_err());
    }

    #[test]
    fn temporal_and_binary_arrays_build() {
        // Previously these fell through to a Utf8 child → ListArray panic.
        let cases = [
            (DataType::Date32, "{2024-01-15,NULL}"),
            (DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), r#"{"2024-01-15 12:30:45+00",NULL}"#),
            (DataType::Binary, r#"{"\\xdead",NULL}"#),
        ];
        for (dt, text) in cases {
            let inner = Arc::new(Field::new("item", dt.clone(), true));
            let arr = build_list(&[Some(text), None], inner, "c").unwrap();
            assert_eq!(arr.len(), 2, "{dt:?}");
            assert_eq!(arr.null_count(), 1, "{dt:?}");
        }
    }

    #[test]
    fn parse_bytea_hex_basic() {
        let bytes = parse_bytea_hex("\\xdeadbeef").unwrap();
        assert_eq!(bytes, vec![0xde, 0xad, 0xbe, 0xef]);
    }
}
