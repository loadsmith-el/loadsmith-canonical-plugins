use anyhow::{bail, Result};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::i256;
use arrow_array::{
    builder::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
        Float32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder, Time32MillisecondBuilder,
        Time32SecondBuilder, Time64MicrosecondBuilder, Time64NanosecondBuilder, FixedSizeBinaryBuilder,
        IntervalMonthDayNanoBuilder,
        TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
        TimestampSecondBuilder,
    },
    cast::AsArray,
    types::{TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType},
    ArrayRef, ListArray, RecordBatch,
};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
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

/// Scale of a `numeric` without a declared precision, unless the source sets
/// `number_scale`.
pub const DEFAULT_NUMBER_SCALE: i8 = 10;

/// Arrow's widest `Decimal128`; a declared precision above it maps to `Decimal256`.
const MAX_DECIMAL128_PRECISION: u8 = 38;
const MAX_DECIMAL256_PRECISION: u8 = 76;

/// Maps a `numeric` column, by its type modifier, to the closest exact Arrow
/// decimal. `numeric(p,s)` is `Decimal128(p, s)` (`Decimal256` past 38 digits);
/// an unconstrained `numeric` (typmod `-1`) is `Decimal128(38, number_scale)`,
/// and a value that doesn't fit it (or `NaN`/`Infinity`) fails the batch
/// instead of being rounded.
pub fn numeric_to_arrow(typmod: i32, number_scale: i8) -> DataType {
    if typmod < 4 {
        return DataType::Decimal128(MAX_DECIMAL128_PRECISION, number_scale);
    }
    // typmod = ((precision << 16) | scale) + VARHDRSZ; the scale is an 11-bit
    // signed field since Postgres 15 (negative scales).
    let t = typmod - 4;
    let precision = ((t >> 16) & 0xffff) as u8;
    let scale = ((((t & 0x7ff) ^ 1024) - 1024) as i16).clamp(i8::MIN as i16, i8::MAX as i16) as i8;
    if scale < 0 {
        // Rounded to the left of the point: whole numbers of up to p - s digits.
        let p = (precision as i16 - scale as i16).min(MAX_DECIMAL256_PRECISION as i16) as u8;
        return decimal_of(p, 0);
    }
    // A scale above the precision (`numeric(2,5)`): Arrow needs precision >= scale.
    decimal_of(precision.max(scale as u8), scale)
}

fn decimal_of(precision: u8, scale: i8) -> DataType {
    if precision <= MAX_DECIMAL128_PRECISION {
        DataType::Decimal128(precision, scale)
    } else {
        DataType::Decimal256(precision.min(MAX_DECIMAL256_PRECISION), scale)
    }
}

/// Field metadata naming an Arrow extension type (`arrow.uuid`).
pub const EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
/// The canonical Arrow UUID extension: `FixedSizeBinary(16)`.
pub const UUID_EXTENSION: &str = "arrow.uuid";

/// How the source maps the types that have a choice.
#[derive(Debug, Clone, Copy)]
pub struct TypeMap {
    /// The timestamps' (and times') unit.
    pub unit: TimeUnit,
    /// The scale of an unconstrained `numeric`.
    pub number_scale: i8,
    /// `uuid` as its text instead of `FixedSizeBinary(16)` (`uuid_as: text`).
    pub uuid_text: bool,
    /// `interval` as its text instead of `Interval(MonthDayNano)`
    /// (`interval_as: text`).
    pub interval_text: bool,
}

impl TypeMap {
    /// The defaults: every type native.
    pub fn new(unit: TimeUnit, number_scale: i8) -> Self {
        Self { unit, number_scale, uuid_text: false, interval_text: false }
    }
}

/// Maps a Postgres type (and its type modifier, which carries `numeric`'s
/// precision and scale) to an Arrow DataType.
///
/// - `array` (any element type) → `DataType::List` with a recursively-mapped inner type
/// - `hstore` → `DataType::Utf8` (JSON-encoded; see HSTORE_TYPE_KEY metadata on the Field)
/// - `numeric` → an exact decimal (see [`numeric_to_arrow`])
/// - `time` → `Time32`/`Time64` in the timestamps' unit
/// - `uuid` → `FixedSizeBinary(16)` (the field carries the `arrow.uuid` extension)
/// - `interval` → `Interval(MonthDayNano)`
/// - everything else → mapped by type name (JSON/text/etc. fall through to Utf8)
pub fn pg_type_to_arrow(pg_type: &PgType, typmod: i32, map: &TypeMap) -> DataType {
    arrow_field("", pg_type, typmod, map).data_type().clone()
}

/// The Arrow field of a column (or array element) named `name`, with the
/// metadata its type needs (hstore, the UUID extension).
fn arrow_field(name: &str, pg_type: &PgType, typmod: i32, map: &TypeMap) -> Field {
    let field = |dt: DataType| Field::new(name, dt, true);
    let tagged = |dt: DataType, key: &str, value: &str| {
        field(dt).with_metadata(HashMap::from([(key.to_string(), value.to_string())]))
    };
    if pg_type.name() == "hstore" {
        return tagged(DataType::Utf8, HSTORE_TYPE_KEY, HSTORE_TYPE_VALUE);
    }
    if let Kind::Array(inner) = pg_type.kind() {
        // An array column's typmod is its element's.
        return field(DataType::List(Arc::new(arrow_field("item", inner, typmod, map))));
    }
    let unit = map.unit;
    field(match pg_type.name() {
        "numeric" => numeric_to_arrow(typmod, map.number_scale),
        "int2" | "int4" => DataType::Int32,
        "int8" => DataType::Int64,
        "float4" => DataType::Float32,
        "float8" => DataType::Float64,
        "bool" => DataType::Boolean,
        "date" => DataType::Date32,
        // A time of day, in the timestamps' unit (Arrow keeps seconds and
        // milliseconds in 32 bits).
        "time" => match unit {
            TimeUnit::Second | TimeUnit::Millisecond => DataType::Time32(unit),
            TimeUnit::Microsecond | TimeUnit::Nanosecond => DataType::Time64(unit),
        },
        "timestamp" => DataType::Timestamp(unit, None),
        // An absolute instant: decoded to UTC, and tagged so downstream (e.g.
        // Parquet) keeps it zone-aware.
        "timestamptz" => DataType::Timestamp(unit, Some("UTC".into())),
        "bytea" => DataType::Binary,
        "uuid" if !map.uuid_text => {
            return tagged(DataType::FixedSizeBinary(16), EXTENSION_NAME_KEY, UUID_EXTENSION);
        }
        "interval" if !map.interval_text => DataType::Interval(IntervalUnit::MonthDayNano),
        _ => DataType::Utf8,
    })
}

/// Builds an Arrow Schema from a slice of tokio-postgres columns.
pub fn columns_to_schema(cols: &[Column], map: &TypeMap) -> Schema {
    Schema::new(cols.iter().map(|c| arrow_field(c.name(), c.type_(), c.type_modifier(), map)).collect::<Vec<_>>())
}

/// Rows whose values are Postgres' text output: `simple_query` rows, or
/// lines of `COPY … TO STDOUT` (see [`crate::read`]).
pub trait TextRows {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// The text of one value; `None` for SQL NULL.
    fn cell(&self, row: usize, col: usize) -> Option<&str>;
}

impl TextRows for [tokio_postgres::SimpleQueryRow] {
    fn len(&self) -> usize {
        <[_]>::len(self)
    }

    fn cell(&self, row: usize, col: usize) -> Option<&str> {
        self[row].get(col)
    }
}

/// Converts text rows to an Arrow RecordBatch.
pub fn rows_to_batch_text<R: TextRows + ?Sized>(rows: &R, schema: &Schema) -> Result<RecordBatch> {
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| build_column_text(rows, idx, field))
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| anyhow::anyhow!("RecordBatch::try_new: {e}"))
}

fn build_column_text<R: TextRows + ?Sized>(rows: &R, col_idx: usize, field: &Field) -> Result<ArrayRef> {
    let values: Vec<Option<&str>> = (0..rows.len()).map(|r| rows.cell(r, col_idx)).collect();
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
        DataType::Decimal128(precision, scale) => {
            let mut b = Decimal128Builder::with_capacity(values.len())
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| anyhow::anyhow!("col {col_name}: {e}"))?;
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(v) => b.append_value(
                        parse_decimal128(v, *precision, *scale)
                            .map_err(|e| decimal_error(col_name, v, *precision, *scale, e))?,
                    ),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        DataType::Decimal256(precision, scale) => {
            let mut b = Decimal256Builder::with_capacity(values.len())
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| anyhow::anyhow!("col {col_name}: {e}"))?;
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(v) => b.append_value(
                        parse_decimal(v, *precision, *scale)
                            .map_err(|e| decimal_error(col_name, v, *precision, *scale, e))?,
                    ),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        DataType::Date32 => parsed!(Date32Builder, parse_date_days),
        DataType::Timestamp(unit, tz) => {
            let with_offset = tz.is_some();
            let parse = |v: &str| parse_timestamp(v, with_offset, *unit);
            let tz = tz.clone();
            macro_rules! timestamps {
                ($builder:ty, $type:ty) => {
                    Arc::new(parsed!($builder, parse).as_primitive::<$type>().clone().with_timezone_opt(tz))
                        as ArrayRef
                };
            }
            match unit {
                TimeUnit::Second => timestamps!(TimestampSecondBuilder, TimestampSecondType),
                TimeUnit::Millisecond => timestamps!(TimestampMillisecondBuilder, TimestampMillisecondType),
                TimeUnit::Microsecond => timestamps!(TimestampMicrosecondBuilder, TimestampMicrosecondType),
                TimeUnit::Nanosecond => timestamps!(TimestampNanosecondBuilder, TimestampNanosecondType),
            }
        }
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => parsed!(Time32SecondBuilder, |v| parse_time(v, *unit).map(|t| t as i32)),
            _ => parsed!(Time32MillisecondBuilder, |v| parse_time(v, *unit).map(|t| t as i32)),
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => parsed!(Time64MicrosecondBuilder, |v| parse_time(v, *unit)),
            _ => parsed!(Time64NanosecondBuilder, |v| parse_time(v, *unit)),
        },
        DataType::FixedSizeBinary(16) => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
            for v in values {
                match v {
                    None => b.append_null(),
                    Some(v) => b
                        .append_value(parse_uuid(v).map_err(|e| anyhow::anyhow!("col {col_name}: uuid '{v}': {e}"))?)
                        .map_err(|e| anyhow::anyhow!("col {col_name}: {e}"))?,
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            parsed!(IntervalMonthDayNanoBuilder, parse_iso_interval)
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

fn decimal_error(col_name: &str, v: &str, precision: u8, scale: i8, e: anyhow::Error) -> anyhow::Error {
    anyhow::anyhow!(
        "col {col_name}: numeric '{v}' {e} (Decimal({precision}, {scale})); declare the column's \
         precision with a cast in the query (`::numeric(p,s)`, or `::text`/`::float8`), or raise `number_scale`"
    )
}

/// Postgres' exact `numeric` text (`-12.5`, `0.001`, `42`; never an
/// exponent), checked against a column's precision and scale: the parts of
/// its unscaled value at `scale`, without allocating.
struct DecimalText<'a> {
    negative: bool,
    /// The integer digits, leading zeros removed.
    int: &'a str,
    /// The fractional digits, trailing zeros removed (at most `scale`).
    frac: &'a str,
    /// The zeros that pad `frac` up to the scale.
    pad: usize,
}

impl<'a> DecimalText<'a> {
    /// More significant fractional digits than `scale`, more digits than
    /// `precision`, or a `NaN`/`Infinity` is an error: nothing is rounded or
    /// truncated.
    fn parse(s: &'a str, precision: u8, scale: i8) -> Result<Self> {
        let (negative, digits) = match s.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, s),
        };
        let (int_part, frac_part) = digits.split_once('.').unwrap_or((digits, ""));
        if int_part.is_empty() && frac_part.is_empty()
            || !int_part.bytes().chain(frac_part.bytes()).all(|b| b.is_ascii_digit())
        {
            bail!("is not a finite decimal number");
        }
        let scale = scale.max(0) as usize;
        let frac = frac_part.trim_end_matches('0');
        if frac.len() > scale {
            bail!("has {} decimal places, more than the column's scale {scale}", frac.len());
        }
        let int = int_part.trim_start_matches('0');
        // The digits of the unscaled value, as written without leading zeros.
        let needed = if !int.is_empty() {
            int.len() + scale
        } else {
            match frac.trim_start_matches('0') {
                "" => 0,
                significant => scale - (frac.len() - significant.len()),
            }
        };
        if needed > precision as usize {
            bail!("needs {needed} digits, more than the column's precision {precision}");
        }
        Ok(Self { negative, int, frac, pad: scale - frac.len() })
    }

    fn digits(&self) -> impl Iterator<Item = u8> + '_ {
        self.int.bytes().chain(self.frac.bytes()).map(|d| d - b'0').chain(std::iter::repeat_n(0, self.pad))
    }
}

/// The unscaled value of a `numeric` for a `Decimal128` (precision ≤ 38, so
/// it never overflows).
fn parse_decimal128(s: &str, precision: u8, scale: i8) -> Result<i128> {
    let text = DecimalText::parse(s, precision.min(38), scale)?;
    let v = text.digits().fold(0i128, |v, d| v * 10 + d as i128);
    Ok(if text.negative { -v } else { v })
}

/// The unscaled value of a `numeric` for a `Decimal256`.
fn parse_decimal(s: &str, precision: u8, scale: i8) -> Result<i256> {
    let text = DecimalText::parse(s, precision, scale)?;
    let ten = i256::from_i128(10);
    let v = text.digits().fold(i256::ZERO, |v, d| v.wrapping_mul(ten).wrapping_add(i256::from_i128(d as i128)));
    Ok(if text.negative { v.wrapping_neg() } else { v })
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
    let time = match parse_iso_time(time) {
        Some(t) => t,
        // Anything off the fixed layout (a leap second, …) keeps chrono's
        // reading and its error.
        None => NaiveTime::parse_from_str(time, "%H:%M:%S%.f").map_err(|e| anyhow::anyhow!("{e}"))?,
    };
    let utc = NaiveDateTime::new(date, time) - chrono::Duration::seconds(offset_secs);
    crate::vocab::epoch_in(utc, unit).map_err(|e| anyhow::anyhow!(e))
}

/// A `uuid`'s 16 bytes from its text (`a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`;
/// Postgres prints it lower-case and hyphenated).
pub fn parse_uuid(s: &str) -> Result<[u8; 16]> {
    let hex: Vec<u8> = s.bytes().filter(|&b| b != b'-').collect();
    if hex.len() != 32 {
        bail!("not a UUID");
    }
    let mut out = [0u8; 16];
    for (i, pair) in hex.chunks(2).enumerate() {
        let digit = |c: u8| (c as char).to_digit(16).ok_or_else(|| anyhow::anyhow!("not a UUID"));
        out[i] = (digit(pair[0])? * 16 + digit(pair[1])?) as u8;
    }
    Ok(out)
}

/// An `interval` in IntervalStyle `iso_8601` (`P1Y2M3DT4H5M6.5S`, every
/// field signed: `P-1Y-2M3DT-4H`, `PT0S`): months, days, nanoseconds.
pub fn parse_iso_interval(s: &str) -> Result<arrow::datatypes::IntervalMonthDayNano> {
    let body = s.strip_prefix('P').ok_or_else(|| anyhow::anyhow!("not an ISO-8601 interval"))?;
    let (date, time) = body.split_once('T').unwrap_or((body, ""));
    let (mut months, mut days, mut nanos) = (0i64, 0i64, 0i128);
    let overflow = || anyhow::anyhow!("out of range");
    for (part, in_time) in [(date, false), (time, true)] {
        let mut rest = part;
        while !rest.is_empty() {
            let at = rest.find(|c: char| c.is_ascii_alphabetic()).ok_or_else(|| anyhow::anyhow!("a number without its unit"))?;
            let (number, unit) = (&rest[..at], rest.as_bytes()[at]);
            rest = &rest[at + 1..];
            match (in_time, unit) {
                (false, b'Y') => months = months.checked_add(number.parse::<i64>()?.checked_mul(12).ok_or_else(overflow)?).ok_or_else(overflow)?,
                (false, b'M') => months = months.checked_add(number.parse()?).ok_or_else(overflow)?,
                (false, b'W') => days = days.checked_add(number.parse::<i64>()?.checked_mul(7).ok_or_else(overflow)?).ok_or_else(overflow)?,
                (false, b'D') => days = days.checked_add(number.parse()?).ok_or_else(overflow)?,
                (true, b'H') => nanos += i128::from(number.parse::<i64>()?) * 3_600_000_000_000,
                (true, b'M') => nanos += i128::from(number.parse::<i64>()?) * 60_000_000_000,
                (true, b'S') => nanos += seconds_in_nanos(number)?,
                _ => bail!("unexpected `{}`", unit as char),
            }
        }
    }
    Ok(arrow::datatypes::IntervalMonthDayNano::new(
        i32::try_from(months).map_err(|_| overflow())?,
        i32::try_from(days).map_err(|_| overflow())?,
        i64::try_from(nanos).map_err(|_| overflow())?,
    ))
}

/// `-6.5` seconds in nanoseconds (up to nine fractional digits).
fn seconds_in_nanos(s: &str) -> Result<i128> {
    let (negative, digits) = s.strip_prefix('-').map_or((false, s), |d| (true, d));
    let (whole, frac) = digits.split_once('.').unwrap_or((digits, ""));
    if frac.len() > 9 || !frac.bytes().all(|b| b.is_ascii_digit()) {
        bail!("bad seconds `{s}`");
    }
    let v = i128::from(whole.parse::<i64>()?) * 1_000_000_000 + format!("{frac:0<9}").parse::<i128>().unwrap_or(0);
    Ok(if negative { -v } else { v })
}

/// A `time` value (`HH:MM:SS[.f]`, up to `24:00:00`) since midnight, in
/// `unit`; digits below the unit are dropped, like a timestamp's.
fn parse_time(s: &str, unit: TimeUnit) -> Result<i64> {
    let nanos = if s == "24:00:00" {
        86_400 * 1_000_000_000
    } else {
        let t = parse_iso_time(s).ok_or_else(|| anyhow::anyhow!("not a time of day (HH:MM:SS[.f])"))?;
        i64::from(t.num_seconds_from_midnight()) * 1_000_000_000 + i64::from(t.nanosecond())
    };
    Ok(match unit {
        TimeUnit::Second => nanos / 1_000_000_000,
        TimeUnit::Millisecond => nanos / 1_000_000,
        TimeUnit::Microsecond => nanos / 1_000,
        TimeUnit::Nanosecond => nanos,
    })
}

/// The `HH:MM:SS[.f]` layout Postgres prints (DateStyle ISO), up to nine
/// fractional digits; `None` for anything else.
fn parse_iso_time(s: &str) -> Option<NaiveTime> {
    let b = s.as_bytes();
    if b.len() < 8 || b[2] != b':' || b[5] != b':' {
        return None;
    }
    let two = |i: usize| -> Option<u32> {
        let (h, l) = (b[i].wrapping_sub(b'0'), b[i + 1].wrapping_sub(b'0'));
        (h < 10 && l < 10).then(|| (h * 10 + l) as u32)
    };
    let (h, m, sec) = (two(0)?, two(3)?, two(6)?);
    let nano = match &b[8..] {
        [] => 0,
        [b'.', frac @ ..] if (1..=9).contains(&frac.len()) => {
            let mut n = 0u32;
            for &d in frac {
                let d = d.wrapping_sub(b'0');
                if d >= 10 {
                    return None;
                }
                n = n * 10 + d as u32;
            }
            n * 10u32.pow(9 - frac.len() as u32)
        }
        _ => return None,
    };
    NaiveTime::from_hms_nano_opt(h, m, sec, nano)
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
        let dt = pg_type_to_arrow(&Type::INT4_ARRAY, -1, &TypeMap::new(TimeUnit::Millisecond, DEFAULT_NUMBER_SCALE));
        assert!(matches!(dt, DataType::List(_)));
        if let DataType::List(inner) = dt {
            assert_eq!(inner.data_type(), &DataType::Int32);
        }
    }

    #[test]
    fn type_text_becomes_utf8() {
        use tokio_postgres::types::Type;
        let dt = pg_type_to_arrow(&Type::TEXT, -1, &TypeMap::new(TimeUnit::Millisecond, DEFAULT_NUMBER_SCALE));
        assert_eq!(dt, DataType::Utf8);
    }

    #[test]
    fn type_float8_becomes_float64() {
        use tokio_postgres::types::Type;
        let dt = pg_type_to_arrow(&Type::FLOAT8, -1, &TypeMap::new(TimeUnit::Millisecond, DEFAULT_NUMBER_SCALE));
        assert_eq!(dt, DataType::Float64);
    }

    #[test]
    fn numeric_follows_its_type_modifier() {
        // typmod of numeric(p,s) = ((p << 16) | s) + 4.
        let typmod = |p: i32, s: i32| ((p << 16) | (s & 0x7ff)) + 4;
        assert_eq!(numeric_to_arrow(-1, 10), DataType::Decimal128(38, 10));
        assert_eq!(numeric_to_arrow(-1, 4), DataType::Decimal128(38, 4));
        assert_eq!(numeric_to_arrow(typmod(10, 2), 10), DataType::Decimal128(10, 2));
        assert_eq!(numeric_to_arrow(typmod(18, 0), 10), DataType::Decimal128(18, 0));
        assert_eq!(numeric_to_arrow(typmod(50, 5), 10), DataType::Decimal256(50, 5));
        assert_eq!(numeric_to_arrow(typmod(5, -2), 10), DataType::Decimal128(7, 0));
        assert_eq!(numeric_to_arrow(typmod(2, 5), 10), DataType::Decimal128(5, 5));
        use tokio_postgres::types::Type;
        let dt = pg_type_to_arrow(&Type::NUMERIC_ARRAY, typmod(9, 3), &TypeMap::new(TimeUnit::Millisecond, 10));
        let DataType::List(inner) = dt else { panic!("{dt:?}") };
        assert_eq!(inner.data_type(), &DataType::Decimal128(9, 3));
    }

    #[test]
    fn decimal128_matches_the_wide_path() {
        let cases = [
            "0", "-0", "0.0", "00012.3400", ".5", "5.", "-0.000001", "99999999.99", "-12345678.99",
            "1", "10", "100.001", "0.1234567890", "12345678901234567890123456.123456789012",
        ];
        for s in cases {
            for (p, sc) in [(38, 10), (38, 12), (10, 2), (20, 0), (12, 3)] {
                let narrow = parse_decimal128(s, p, sc).map_err(|e| e.to_string());
                let wide = parse_decimal(s, p, sc).map(|d| d.as_i128()).map_err(|e| e.to_string());
                assert_eq!(narrow, wide, "{s} as ({p},{sc})");
            }
        }
        let max = "9".repeat(38);
        assert_eq!(parse_decimal128(&max, 38, 0).unwrap().to_string(), max);
    }

    #[test]
    fn hand_parsed_times_match_chrono() {
        for s in ["00:00:00", "23:59:59", "12:30:45.1", "12:30:45.123456", "01:02:03.000000001", "07:08:09.120"] {
            assert_eq!(parse_iso_time(s), NaiveTime::parse_from_str(s, "%H:%M:%S%.f").ok(), "{s}");
        }
        for s in ["24:00:00", "12:60:00", "1:02:03", "12:30:45.", "12:30:45.1234567890", "12:30:45x", "12:3a:45"] {
            assert_eq!(parse_iso_time(s), None, "{s}");
        }
        // Off the fast path, chrono still decides (a leap second).
        assert!(parse_timestamp("2016-12-31 23:59:60", false, TimeUnit::Second).is_ok());
    }

    #[test]
    fn numeric_text_parses_exactly_or_fails() {
        let v = |s: &str, p, sc| parse_decimal(s, p, sc).map(|d| d.as_i128());
        assert_eq!(v("12345.6789", 38, 10).unwrap(), 123_456_789_000_000);
        assert_eq!(v("-99.95", 10, 2).unwrap(), -9995);
        assert_eq!(v("0", 10, 2).unwrap(), 0);
        assert_eq!(v("-0.000001", 38, 10).unwrap(), -10_000);
        let big = format!("{}.5", "9".repeat(60));
        let d = parse_decimal(&big, 61, 1).unwrap();
        assert_eq!(d.to_string(), format!("{}5", "9".repeat(60)));
        assert!(v("1.234", 10, 2).unwrap_err().to_string().contains("scale 2"));
        assert!(v("123456789012345678901234567890.1", 38, 10).unwrap_err().to_string().contains("precision 38"));
        assert!(v("NaN", 38, 10).unwrap_err().to_string().contains("finite"));
        assert!(v("-Infinity", 38, 10).is_err());
    }

    #[test]
    fn numeric_columns_build_decimals() {
        let field = Field::new("n", DataType::Decimal128(10, 2), true);
        let arr = build_values(&[Some("99.95"), None, Some("-12345678.99")], &field, "n").unwrap();
        use arrow_array::Array;
        let d = arr.as_any().downcast_ref::<arrow_array::Decimal128Array>().unwrap();
        assert_eq!(d.value_as_string(0), "99.95");
        assert!(d.is_null(1));
        assert_eq!(d.value_as_string(2), "-12345678.99");
        let err = build_values(&[Some("NaN")], &field, "n").unwrap_err().to_string();
        assert!(err.contains("number_scale") && err.contains("col n"), "{err}");
        let wide = Field::new("w", DataType::Decimal256(50, 0), true);
        let arr = build_values(&[Some(&"7".repeat(50))], &wide, "w").unwrap();
        let d = arr.as_any().downcast_ref::<arrow_array::Decimal256Array>().unwrap();
        assert_eq!(d.value_as_string(0), "7".repeat(50));
    }

    // ── parse_timestamp_millis / parse_bytea_hex (unchanged) ─────────────────

    #[test]
    fn uuids_are_16_bytes_tagged_as_the_arrow_extension() {
        let bytes = parse_uuid("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap();
        assert_eq!(bytes[..4], [0xa0, 0xee, 0xbc, 0x99]);
        assert_eq!(parse_uuid("A0EEBC999C0B4EF8BB6D6BB9BD380A11").unwrap(), bytes);
        assert!(parse_uuid("a0eebc99").is_err());
        assert!(parse_uuid("g0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").is_err());
        let map = TypeMap::new(TimeUnit::Millisecond, 10);
        let f = arrow_field("id", &PgType::UUID, -1, &map);
        assert_eq!(f.data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(f.metadata().get(EXTENSION_NAME_KEY).map(String::as_str), Some(UUID_EXTENSION));
        let list = arrow_field("ids", &PgType::UUID_ARRAY, -1, &map);
        let DataType::List(item) = list.data_type() else { panic!("{list:?}") };
        assert_eq!(item.metadata().get(EXTENSION_NAME_KEY).map(String::as_str), Some(UUID_EXTENSION));
        let text = TypeMap { uuid_text: true, ..map };
        assert_eq!(pg_type_to_arrow(&PgType::UUID, -1, &text), DataType::Utf8);
    }

    #[test]
    fn intervals_are_months_days_and_nanoseconds() {
        use arrow::datatypes::IntervalMonthDayNano as I;
        assert_eq!(parse_iso_interval("P1Y2M3DT4H5M6.5S").unwrap(), I::new(14, 3, 14_706_500_000_000));
        assert_eq!(parse_iso_interval("P-1Y-2M3DT-4H-5M-6.000001S").unwrap(), I::new(-14, 3, -14_706_000_001_000));
        assert_eq!(parse_iso_interval("PT0S").unwrap(), I::new(0, 0, 0));
        assert_eq!(parse_iso_interval("P2W").unwrap(), I::new(0, 14, 0));
        assert_eq!(parse_iso_interval("PT-0.5S").unwrap(), I::new(0, 0, -500_000_000));
        assert!(parse_iso_interval("1 day").is_err());
        assert!(parse_iso_interval("P1X").is_err());
        let map = TypeMap::new(TimeUnit::Millisecond, 10);
        assert_eq!(pg_type_to_arrow(&PgType::INTERVAL, -1, &map), DataType::Interval(IntervalUnit::MonthDayNano));
        assert_eq!(pg_type_to_arrow(&PgType::INTERVAL, -1, &TypeMap { interval_text: true, ..map }), DataType::Utf8);
    }

    #[test]
    fn time_is_a_time_of_day_in_the_unit() {
        assert_eq!(parse_time("13:45:30.123456", TimeUnit::Millisecond).unwrap(), 49_530_123);
        assert_eq!(parse_time("13:45:30.123456", TimeUnit::Microsecond).unwrap(), 49_530_123_456);
        assert_eq!(parse_time("00:00:00", TimeUnit::Second).unwrap(), 0);
        assert_eq!(parse_time("24:00:00", TimeUnit::Microsecond).unwrap(), 86_400_000_000);
        assert!(parse_time("13:45", TimeUnit::Millisecond).is_err());
        let t = PgType::TIME;
        assert_eq!(pg_type_to_arrow(&t, -1, &TypeMap::new(TimeUnit::Millisecond, 10)), DataType::Time32(TimeUnit::Millisecond));
        assert_eq!(pg_type_to_arrow(&t, -1, &TypeMap::new(TimeUnit::Microsecond, 10)), DataType::Time64(TimeUnit::Microsecond));
        let values = [Some("01:02:03.5"), None];
        let a = build_values(&values, &Field::new("t", DataType::Time32(TimeUnit::Millisecond), true), "t").unwrap();
        assert_eq!(a.as_primitive::<arrow_array::types::Time32MillisecondType>().value(0), 3_723_500);
        assert!(a.is_null(1));
    }

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
