use anyhow::{bail, Result};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow_array::{
    builder::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
        Int32Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
    },
    ArrayRef, ListArray, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
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
    *UNIX_EPOCH.get_or_init(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
}

/// Maps a Postgres type to an Arrow DataType.
///
/// - `array` (any element type) → `DataType::List` with a recursively-mapped inner type
/// - `hstore` → `DataType::Utf8` (JSON-encoded; see HSTORE_TYPE_KEY metadata on the Field)
/// - everything else → mapped by type name (NUMERIC/UUID/etc. fall through to Utf8)
pub fn pg_type_to_arrow(pg_type: &PgType) -> DataType {
    if pg_type.name() == "hstore" {
        return DataType::Utf8;
    }
    if let Kind::Array(inner) = pg_type.kind() {
        let inner_dt = pg_type_to_arrow(inner);
        return DataType::List(Arc::new(Field::new("item", inner_dt, true)));
    }
    match pg_type.name() {
        "int2" | "int4" => DataType::Int32,
        "int8" => DataType::Int64,
        "float4" => DataType::Float32,
        "float8" => DataType::Float64,
        "bool" => DataType::Boolean,
        "date" => DataType::Date32,
        "timestamp" | "timestamptz" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "bytea" => DataType::Binary,
        _ => DataType::Utf8,
    }
}

/// Builds an Arrow Schema from a slice of tokio-postgres columns.
pub fn columns_to_schema(cols: &[Column]) -> Schema {
    let fields: Vec<Field> = cols
        .iter()
        .map(|c| {
            let dt = pg_type_to_arrow(c.type_());
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
    let col_name = field.name();
    match field.data_type() {
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(rows.len());
            for row in rows {
                match row.get(col_idx) {
                    None => b.append_null(),
                    Some(v) => b.append_value(
                        v.parse::<i32>()
                            .map_err(|e| anyhow::anyhow!("col {col_name}: int32 parse '{v}': {e}"))?,
                    ),
                }
            }
            Ok(Arc::new(b.finish()))
        }

        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match row.get(col_idx) {
                    None => b.append_null(),
                    Some(v) => b.append_value(
                        v.parse::<i64>()
                            .map_err(|e| anyhow::anyhow!("col {col_name}: int64 parse '{v}': {e}"))?,
                    ),
                }
            }
            Ok(Arc::new(b.finish()))
        }

        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for row in rows {
                match row.get(col_idx) {
                    None => b.append_null(),
                    Some(v) => b.append_value(
                        v.parse::<f32>()
                            .map_err(|e| anyhow::anyhow!("col {col_name}: float32 parse '{v}': {e}"))?,
                    ),
                }
            }
            Ok(Arc::new(b.finish()))
        }

        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match row.get(col_idx) {
                    None => b.append_null(),
                    Some(v) => b.append_value(
                        v.parse::<f64>()
                            .map_err(|e| anyhow::anyhow!("col {col_name}: float64 parse '{v}': {e}"))?,
                    ),
                }
            }
            Ok(Arc::new(b.finish()))
        }

        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.get(col_idx) {
                    None => b.append_null(),
                    Some("t") | Some("true") => b.append_value(true),
                    Some("f") | Some("false") => b.append_value(false),
                    Some(v) => bail!("col {col_name}: unexpected bool value '{v}'"),
                }
            }
            Ok(Arc::new(b.finish()))
        }

        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(rows.len());
            let epoch = unix_epoch();
            for row in rows {
                match row.get(col_idx) {
                    None => b.append_null(),
                    Some(v) => {
                        let d = NaiveDate::parse_from_str(v, "%Y-%m-%d")
                            .map_err(|e| anyhow::anyhow!("col {col_name}: date parse '{v}': {e}"))?;
                        b.append_value(d.signed_duration_since(epoch).num_days() as i32);
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }

        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let mut b = TimestampMillisecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.get(col_idx) {
                    None => b.append_null(),
                    Some(v) => {
                        let ms = parse_timestamp_millis(v)
                            .map_err(|e| anyhow::anyhow!("col {col_name}: ts parse '{v}': {e}"))?;
                        b.append_value(ms);
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }

        DataType::Binary => {
            let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
            for row in rows {
                match row.get(col_idx) {
                    None => b.append_null(),
                    Some(v) => b.append_value(parse_bytea_hex(v)?),
                }
            }
            Ok(Arc::new(b.finish()))
        }

        DataType::Utf8 => {
            let is_hstore = field
                .metadata()
                .get(HSTORE_TYPE_KEY)
                .map(String::as_str)
                == Some(HSTORE_TYPE_VALUE);

            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                match row.get(col_idx) {
                    None => b.append_null(),
                    Some(v) => {
                        if is_hstore {
                            b.append_value(parse_hstore_to_json(v)
                                .map_err(|e| anyhow::anyhow!("col {col_name}: hstore parse: {e}"))?)
                        } else {
                            b.append_value(v)
                        }
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }

        DataType::List(inner_field) => {
            build_list_column_text(rows, col_idx, inner_field.clone(), col_name)
        }

        other => bail!("unsupported arrow type: {other:?}"),
    }
}

/// Builds a `ListArray` column from Postgres text-protocol array notation (`{1,2,3}`).
fn build_list_column_text(
    rows: &[tokio_postgres::SimpleQueryRow],
    col_idx: usize,
    inner_field: Arc<Field>,
    col_name: &str,
) -> Result<ArrayRef> {
    let mut lengths: Vec<usize> = Vec::with_capacity(rows.len());
    let mut is_valid: Vec<bool> = Vec::with_capacity(rows.len());
    let mut all_items: Vec<Option<String>> = Vec::new();

    for row in rows {
        match row.get(col_idx) {
            None => {
                lengths.push(0);
                is_valid.push(false);
            }
            Some(text) => {
                let items = parse_pg_array_text(text);
                lengths.push(items.len());
                is_valid.push(true);
                all_items.extend(items);
            }
        }
    }

    let child = build_flat_scalars(&all_items, inner_field.data_type(), col_name)?;
    let offsets = OffsetBuffer::from_lengths(lengths);
    let nulls = Some(NullBuffer::from(is_valid.as_slice()));
    Ok(Arc::new(ListArray::new(inner_field, offsets, child, nulls)))
}

/// Builds a flat Arrow array from `Vec<Option<String>>` text values using the given type.
fn build_flat_scalars(
    values: &[Option<String>],
    dtype: &DataType,
    col_name: &str,
) -> Result<ArrayRef> {
    match dtype {
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(values.len());
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(s) => b.append_value(
                        s.parse::<i32>()
                            .map_err(|e| anyhow::anyhow!("col {col_name}[]: int32 '{s}': {e}"))?,
                    ),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(s) => b.append_value(
                        s.parse::<i64>()
                            .map_err(|e| anyhow::anyhow!("col {col_name}[]: int64 '{s}': {e}"))?,
                    ),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(values.len());
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(s) => b.append_value(
                        s.parse::<f32>()
                            .map_err(|e| anyhow::anyhow!("col {col_name}[]: float32 '{s}': {e}"))?,
                    ),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(s) => b.append_value(
                        s.parse::<f64>()
                            .map_err(|e| anyhow::anyhow!("col {col_name}[]: float64 '{s}': {e}"))?,
                    ),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v.as_deref() {
                    None => b.append_null(),
                    Some("t") | Some("true") => b.append_value(true),
                    Some("f") | Some("false") => b.append_value(false),
                    Some(s) => bail!("col {col_name}[]: unexpected bool '{s}'"),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        _ => {
            let mut b = StringBuilder::with_capacity(values.len(), values.len() * 16);
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(s) => b.append_value(s),
                }
            }
            Ok(Arc::new(b.finish()))
        }
    }
}

/// Parses the Postgres text-protocol array format `{val1,val2,NULL,...}` into a Vec.
/// Quoted elements (`"..."`) have backslash-escape handling; unquoted `NULL` → `None`.
fn parse_pg_array_text(text: &str) -> Vec<Option<String>> {
    let text = text.trim();
    let inner = match text.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
        Some(s) => s,
        None => return vec![Some(text.to_string())],
    };
    if inner.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::new();
    let mut chars = inner.chars().peekable();

    loop {
        while chars.peek() == Some(&' ') {
            chars.next();
        }
        if chars.peek().is_none() {
            break;
        }

        if chars.peek() == Some(&'"') {
            chars.next(); // consume opening "
            let mut value = String::new();
            loop {
                match chars.next() {
                    None => break,
                    Some('\\') => {
                        if let Some(c) = chars.next() {
                            value.push(c);
                        }
                    }
                    Some('"') => break,
                    Some(c) => value.push(c),
                }
            }
            result.push(Some(value));
        } else {
            let mut value = String::new();
            while let Some(&c) = chars.peek() {
                if c == ',' {
                    break;
                }
                value.push(c);
                chars.next();
            }
            let value = value.trim().to_string();
            if value.eq_ignore_ascii_case("null") {
                result.push(None);
            } else {
                result.push(Some(value));
            }
        }

        while chars.peek() == Some(&' ') {
            chars.next();
        }
        if chars.peek() == Some(&',') {
            chars.next();
        } else {
            break;
        }
    }

    result
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

/// Parses postgres timestamp text ("YYYY-MM-DD HH:MM:SS[.ffffff]") → milliseconds since epoch.
fn parse_timestamp_millis(s: &str) -> Result<i64> {
    let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(dt.and_utc().timestamp_millis())
}

/// Parses postgres bytea hex format ("\\xdeadbeef") → bytes.
fn parse_bytea_hex(s: &str) -> Result<Vec<u8>> {
    let hex = s.strip_prefix("\\x").unwrap_or(s);
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|e| anyhow::anyhow!("{e}")))
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_pg_array_text ──────────────────────────────────────────────────

    #[test]
    fn array_empty() {
        assert_eq!(parse_pg_array_text("{}"), Vec::<Option<String>>::new());
    }

    #[test]
    fn array_integers() {
        let v = parse_pg_array_text("{1,2,3}");
        assert_eq!(v, vec![Some("1".into()), Some("2".into()), Some("3".into())]);
    }

    #[test]
    fn array_text_quoted() {
        let v = parse_pg_array_text(r#"{"hello","world"}"#);
        assert_eq!(v, vec![Some("hello".into()), Some("world".into())]);
    }

    #[test]
    fn array_text_with_spaces_in_value() {
        let v = parse_pg_array_text(r#"{"hello world","foo bar"}"#);
        assert_eq!(v, vec![Some("hello world".into()), Some("foo bar".into())]);
    }

    #[test]
    fn array_with_null_element() {
        let v = parse_pg_array_text("{1,NULL,3}");
        assert_eq!(v, vec![Some("1".into()), None, Some("3".into())]);
    }

    #[test]
    fn array_quoted_with_backslash_escape() {
        let v = parse_pg_array_text(r#"{"say \"hi\""}"#);
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
        let dt = pg_type_to_arrow(&Type::INT4_ARRAY);
        assert!(matches!(dt, DataType::List(_)));
        if let DataType::List(inner) = dt {
            assert_eq!(inner.data_type(), &DataType::Int32);
        }
    }

    #[test]
    fn type_text_becomes_utf8() {
        use tokio_postgres::types::Type;
        let dt = pg_type_to_arrow(&Type::TEXT);
        assert_eq!(dt, DataType::Utf8);
    }

    #[test]
    fn type_float8_becomes_float64() {
        use tokio_postgres::types::Type;
        let dt = pg_type_to_arrow(&Type::FLOAT8);
        assert_eq!(dt, DataType::Float64);
    }

    // ── parse_timestamp_millis / parse_bytea_hex (unchanged) ─────────────────

    #[test]
    fn parse_timestamp_millis_basic() {
        let ms = parse_timestamp_millis("2024-01-15 12:30:45.123").unwrap();
        assert!(ms > 0);
    }

    #[test]
    fn parse_bytea_hex_basic() {
        let bytes = parse_bytea_hex("\\xdeadbeef").unwrap();
        assert_eq!(bytes, vec![0xde, 0xad, 0xbe, 0xef]);
    }
}
