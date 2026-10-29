//! Oracle ⇄ Arrow type mapping, for the pure-Rust `oracledb` thin driver.
//!
//! `NUMBER` crosses as the closest exact Arrow number (see [`number_to_arrow`]):
//! `NUMBER(p)` as `Int32`/`Int64` (or `Decimal128(p, 0)` past 18 digits),
//! `NUMBER(p,s)` as `Decimal128(p, s)`, a `NUMBER` without a declared precision
//! as `Decimal128(38, number_scale)`, `FLOAT(b)` as `Float64`. `INTERVAL YEAR
//! TO MONTH` / `DAY TO SECOND` as `Interval(MonthDayNano)` (or their text, with
//! `interval_as: text`); `ROWID` and the character types cross as exact text
//! (`Utf8`); `RAW`/`BLOB`
//! as `Binary`; the IEEE `BINARY_FLOAT`/`BINARY_DOUBLE` as Arrow floats; `DATE`, `TIMESTAMP` and
//! `TIMESTAMP WITH [LOCAL] TIME ZONE` as Arrow timestamps in the source's
//! `timestamp_unit` (the zoned ones normalized to UTC).
//!
//! `DATE` and `TIMESTAMP` are fetched natively (Oracle's 7/11-byte binary form);
//! the zoned timestamps are normalized server-side with `SYS_EXTRACT_UTC` and
//! fetched the same way; CLOB/LONG whole, inline. INTERVAL is rendered with `TO_CHAR` (see
//! [`projected_select`]). The destination wraps temporal binds in the matching
//! `TO_DATE`/`TO_TIMESTAMP`/`TO_TIMESTAMP_TZ` ([`typed_expr`]).

use anyhow::{bail, Context, Result};
use arrow::array::AsArray;
use arrow::datatypes::{
    Date32Type, Date64Type, IntervalDayTimeType, IntervalMonthDayNano, IntervalMonthDayNanoType,
    IntervalYearMonthType, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
    builder::{
        BinaryBuilder, BooleanBuilder, Decimal128Builder, IntervalMonthDayNanoBuilder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, StringBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
        TimestampNanosecondBuilder, TimestampSecondBuilder,
    },
    Array, ArrayRef, Decimal128Array, Decimal256Array, RecordBatch,
};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use chrono::NaiveDateTime;
use oracledb::{DbType, Row, ToDbValue};
use std::borrow::Cow;
use std::sync::Arc;

use crate::conn::{FMT_DATE, FMT_TIMESTAMP_IN, FMT_TIMESTAMP_TZ_IN};
use crate::vocab::{epoch_in, from_epoch, time_of_day};

// ── Schema inference (source) ───────────────────────────────────────────────

/// Scale of a `NUMBER` without a declared precision (a bare `NUMBER`
/// column, or a computed one such as `SUM(x)`), unless the source sets
/// `number_scale`.
pub const DEFAULT_NUMBER_SCALE: i8 = 10;

/// Arrow's widest exact decimal (`Decimal128`), and Oracle's `NUMBER` maximum.
const MAX_DECIMAL_PRECISION: u8 = 38;

/// Maps a `NUMBER` column, by its declared precision and scale, to the closest
/// exact Arrow type. Oracle reports precision `0` when none was declared, and
/// scale `-127` for a floating scale (`FLOAT(b)`, or a bare `NUMBER`).
pub fn number_to_arrow(precision: u8, scale: i8, number_scale: i8) -> DataType {
    match (precision, scale) {
        // FLOAT(b): binary precision, no fixed scale — the closest is an IEEE double.
        (p, -127) if p > 0 => DataType::Float64,
        // No declared precision: up to 38 significant digits. A value with more
        // fractional digits than `number_scale` fails the batch instead of
        // being rounded.
        (0, -127) => DataType::Decimal128(MAX_DECIMAL_PRECISION, number_scale),
        (0, s) => DataType::Decimal128(MAX_DECIMAL_PRECISION, s.max(0)),
        (p, 0) if p <= 9 => DataType::Int32,
        (p, 0) if p <= 18 => DataType::Int64,
        // A negative scale rounds to the left of the point: whole numbers of up
        // to `p - s` digits.
        (p, s) if s < 0 => DataType::Decimal128((p as i16 - s as i16).min(MAX_DECIMAL_PRECISION as i16) as u8, 0),
        // Oracle allows a scale above the precision (`NUMBER(2,5)` = 0.000dd);
        // Arrow needs precision >= scale.
        (p, s) => DataType::Decimal128(p.max(s as u8).min(MAX_DECIMAL_PRECISION), s),
    }
}

/// Maps a described column to an Arrow `DataType`.
pub fn column_to_arrow(c: &ColumnDesc, unit: TimeUnit, number_scale: i8) -> DataType {
    if c.oracle_type == oracledb::DB_TYPE_NUMBER {
        number_to_arrow(c.precision, c.scale, number_scale)
    } else {
        oracle_to_arrow(c.oracle_type, unit)
    }
}

/// Maps an Oracle column type to an Arrow `DataType`. `NUMBER` needs its
/// precision and scale: see [`column_to_arrow`].
pub fn oracle_to_arrow(t: &DbType, unit: TimeUnit) -> DataType {
    if t == oracledb::DB_TYPE_NUMBER {
        number_to_arrow(0, -127, DEFAULT_NUMBER_SCALE)
    } else if t == oracledb::DB_TYPE_BINARY_INTEGER {
        DataType::Int32
    } else if t == oracledb::DB_TYPE_BINARY_FLOAT {
        DataType::Float32
    } else if t == oracledb::DB_TYPE_BINARY_DOUBLE {
        DataType::Float64
    } else if is_one_of(t, &[oracledb::DB_TYPE_RAW, oracledb::DB_TYPE_LONG_RAW, oracledb::DB_TYPE_BLOB, oracledb::DB_TYPE_BFILE]) {
        DataType::Binary
    } else if t == oracledb::DB_TYPE_BOOLEAN {
        DataType::Boolean
    } else if is_one_of(t, &[oracledb::DB_TYPE_DATE, oracledb::DB_TYPE_TIMESTAMP]) {
        DataType::Timestamp(unit, None)
    } else if is_zoned(t) {
        DataType::Timestamp(unit, Some("UTC".into()))
    } else {
        // INTERVAL, CHAR/VARCHAR2/CLOB/LONG, ROWID, JSON, and anything exotic
        // → text-preserving Utf8.
        DataType::Utf8
    }
}

/// `DbType` only derives `PartialEq` (no structural match), so type tests are
/// done by comparison against the driver's `DB_TYPE_*` constants.
fn is_one_of(t: &DbType, set: &[&DbType]) -> bool {
    set.contains(&t)
}

fn is_interval(t: &DbType) -> bool {
    is_one_of(t, &[oracledb::DB_TYPE_INTERVAL_YM, oracledb::DB_TYPE_INTERVAL_DS])
}

fn is_zoned(t: &DbType) -> bool {
    is_one_of(t, &[oracledb::DB_TYPE_TIMESTAMP_TZ, oracledb::DB_TYPE_TIMESTAMP_LTZ])
}

/// Whether a column must be read through an expression. Returns the
/// expression to use, or `None` to read the column directly.
fn projected_expr(col: &str, t: &DbType) -> Option<String> {
    let quoted = quote_ident(col);
    if t == oracledb::DB_TYPE_TIMESTAMP_TZ {
        // The UTC instant as a plain TIMESTAMP, fetched natively. (The driver
        // can't decode a zone region, and doesn't apply the offset.)
        Some(format!("SYS_EXTRACT_UTC({quoted})"))
    } else if t == oracledb::DB_TYPE_TIMESTAMP_LTZ {
        // Through the session time zone to an instant; `(9)` keeps every digit.
        Some(format!("SYS_EXTRACT_UTC(CAST({quoted} AS TIMESTAMP(9) WITH TIME ZONE))"))
    } else if is_one_of(
        t,
        &[oracledb::DB_TYPE_INTERVAL_YM, oracledb::DB_TYPE_INTERVAL_DS],
    ) {
        // INTERVAL renders to text with the default model. (CLOB and LONG are
        // fetched whole, inline — the driver describes a CLOB as LONG — where
        // `TO_CHAR` would stop a CLOB at 4000 bytes and reject a LONG.)
        Some(format!("TO_CHAR({quoted})"))
    } else {
        None
    }
}

/// Builds the projected `SELECT` list and the matching Arrow `Schema` from the
/// described columns. Zoned timestamps and INTERVAL columns are
/// wrapped (`SYS_EXTRACT_UTC(...)`, `TO_CHAR(...)`) `AS "name"`; every other
/// column is selected verbatim. All fields are nullable.
pub fn projected_select(cols: &[ColumnDesc], unit: TimeUnit, number_scale: i8, interval_text: bool) -> (String, Schema) {
    let mut select = Vec::with_capacity(cols.len());
    let mut fields = Vec::with_capacity(cols.len());
    for c in cols {
        let expr = match projected_expr(&c.name, c.oracle_type) {
            Some(e) => format!("{e} AS {}", quote_ident(&c.name)),
            None => quote_ident(&c.name),
        };
        select.push(expr);
        let dt = if is_interval(c.oracle_type) && !interval_text {
            DataType::Interval(IntervalUnit::MonthDayNano)
        } else {
            column_to_arrow(c, unit, number_scale)
        };
        fields.push(Field::new(&c.name, dt, true));
    }
    (select.join(", "), Schema::new(fields))
}

/// A minimal, owned description of one result column (name, Oracle type, and
/// the declared precision/scale that type `NUMBER`).
#[derive(Debug, Clone)]
pub struct ColumnDesc {
    pub name: String,
    pub oracle_type: &'static DbType,
    pub precision: u8,
    pub scale: i8,
}

impl ColumnDesc {
    /// A column with no precision or scale (anything but `NUMBER`).
    pub fn new(name: impl Into<String>, oracle_type: &'static DbType) -> Self {
        Self { name: name.into(), oracle_type, precision: 0, scale: 0 }
    }
}

/// Extracts column descriptions from a result set's column metadata (obtained by
/// running the query with a `WHERE 1 = 0` describe wrapper).
pub fn describe(cols: &[oracledb::Metadata]) -> Vec<ColumnDesc> {
    cols.iter()
        .map(|c| ColumnDesc {
            name: c.name().to_string(),
            oracle_type: c.db_type(),
            precision: c.precision(),
            scale: c.scale(),
        })
        .collect()
}

/// Wraps a SQL token (`:1`, or a quoted literal) in the explicit `TO_*`
/// conversion for a temporal Oracle type, so text→value conversion never relies
/// on the session's NLS settings. Non-temporal types pass through unchanged
/// (Oracle's implicit char→NUMBER/char→CLOB conversion handles them).
pub fn typed_expr(inner: &str, t: &DbType) -> String {
    if t == oracledb::DB_TYPE_DATE {
        format!("TO_DATE({inner}, '{FMT_DATE}')")
    } else if t == oracledb::DB_TYPE_TIMESTAMP {
        format!("TO_TIMESTAMP({inner}, '{FMT_TIMESTAMP_IN}')")
    } else if is_zoned(t) {
        format!("TO_TIMESTAMP_TZ({inner}, '{FMT_TIMESTAMP_TZ_IN}')")
    } else if t == oracledb::DB_TYPE_INTERVAL_YM {
        format!("TO_YMINTERVAL({inner})")
    } else if t == oracledb::DB_TYPE_INTERVAL_DS {
        format!("TO_DSINTERVAL({inner})")
    } else {
        inner.to_string()
    }
}

// ── Row values (source) ─────────────────────────────────────────────────────

/// One fetched Oracle value in its native form, borrowed from the fetched row
/// where it can be: a number keeps its `OracleNumber`, text and bytes are not
/// copied, a temporal keeps its UTC wall clock.
#[derive(Debug, Clone)]
pub enum Value<'a> {
    Null,
    Number(oracledb::OracleNumber),
    F32(f32),
    F64(f64),
    Bool(bool),
    Temporal(NaiveDateTime),
    Bytes(Cow<'a, [u8]>),
    Text(Cow<'a, str>),
}

impl Value<'_> {
    /// The value as text, rendered into `buf` when it is not text already:
    /// a number's exact decimal text (NLS-independent), a float's shortest
    /// round-trip text, a boolean as `1`/`0`. `None` for NULL, raw bytes and
    /// temporals.
    pub fn text<'s>(&'s self, buf: &'s mut String) -> Option<&'s str> {
        use std::fmt::Write;
        buf.clear();
        match self {
            Value::Text(s) => return Some(s),
            Value::Bool(b) => return Some(if *b { "1" } else { "0" }),
            Value::Number(n) => write!(buf, "{n}"),
            Value::F32(f) => write!(buf, "{f}"),
            Value::F64(f) => write!(buf, "{f}"),
            Value::Null | Value::Bytes(_) | Value::Temporal(_) => return None,
        }
        .ok()?;
        Some(buf)
    }

    /// The incremental watermark of a cursor column of type `t`. A temporal
    /// value is rendered as text in the format `typed_expr` reads back (the
    /// same text earlier versions persisted, so saved state still resumes).
    pub fn watermark(&self, t: Option<&DbType>) -> Option<String> {
        match self {
            Value::Temporal(dt) => Some(match t {
                Some(t) if t == oracledb::DB_TYPE_DATE => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                Some(t) if is_zoned(t) => dt.format("%Y-%m-%d %H:%M:%S%.9f +00:00").to_string(),
                _ => dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
            }),
            other => other.text(&mut String::new()).map(str::to_string),
        }
    }
}

/// A fetched row, as the batch builder reads it.
pub trait FetchedRow {
    /// The value of column `idx`.
    fn value(&self, idx: usize) -> Result<Value<'_>>;
}

impl FetchedRow for Row {
    fn value(&self, idx: usize) -> Result<Value<'_>> {
        read_value(self, idx)
    }
}

/// Rows built by hand (tests): a short row reads as NULL.
impl FetchedRow for Vec<Value<'static>> {
    fn value(&self, idx: usize) -> Result<Value<'_>> {
        Ok(self.get(idx).cloned().unwrap_or(Value::Null))
    }
}

/// Reads column `i` of a fetched row, dispatching on the column's *fetched*
/// type (the projected query's, so `TO_CHAR`-wrapped columns arrive as VARCHAR
/// and `SYS_EXTRACT_UTC` ones as TIMESTAMP).
pub fn read_value(row: &Row, i: usize) -> Result<Value<'_>> {
    let meta = row.columns().get(i).context("column index out of range")?;
    let t = meta.db_type();
    let err = |e: oracledb::Error| anyhow::anyhow!("col {}: {e}", meta.name());
    Ok(if is_one_of(t, &[oracledb::DB_TYPE_NUMBER, oracledb::DB_TYPE_BINARY_INTEGER]) {
        row.get::<Option<oracledb::OracleNumber>>(i).map_err(err)?.map_or(Value::Null, Value::Number)
    } else if t == oracledb::DB_TYPE_BINARY_FLOAT {
        row.get::<Option<f32>>(i).map_err(err)?.map_or(Value::Null, Value::F32)
    } else if t == oracledb::DB_TYPE_BINARY_DOUBLE {
        row.get::<Option<f64>>(i).map_err(err)?.map_or(Value::Null, Value::F64)
    } else if t == oracledb::DB_TYPE_BOOLEAN {
        row.get::<Option<bool>>(i).map_err(err)?.map_or(Value::Null, Value::Bool)
    } else if is_one_of(t, &[oracledb::DB_TYPE_DATE, oracledb::DB_TYPE_TIMESTAMP]) {
        match row.get::<Option<oracledb::OracleTimestamp>>(i).map_err(err)? {
            Some(ts) => Value::Temporal(NaiveDateTime::try_from(ts).map_err(err)?),
            None => Value::Null,
        }
    } else if is_one_of(t, &[oracledb::DB_TYPE_RAW, oracledb::DB_TYPE_LONG_RAW, oracledb::DB_TYPE_BLOB]) {
        // BLOBs are fetched inline as bytes (no `fetch_lobs()` on the statement).
        match row.get::<Option<&[u8]>>(i) {
            Ok(b) => b.map_or(Value::Null, |b| Value::Bytes(Cow::Borrowed(b))),
            Err(_) => row.get::<Option<Vec<u8>>>(i).map_err(err)?.map_or(Value::Null, |b| Value::Bytes(Cow::Owned(b))),
        }
    } else {
        // VARCHAR/CHAR/N*, inline CLOB, every TO_CHAR projection; a ROWID
        // only renders to an owned string.
        match row.get::<Option<&str>>(i) {
            Ok(s) => s.map_or(Value::Null, |s| Value::Text(Cow::Borrowed(s))),
            Err(_) => row.get::<Option<String>>(i).map_err(err)?.map_or(Value::Null, |s| Value::Text(Cow::Owned(s))),
        }
    })
}

/// Converts a slice of fetched rows into an Arrow `RecordBatch`.
pub fn rows_to_batch<R: FetchedRow>(rows: &[R], schema: &Schema) -> Result<RecordBatch> {
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| build_column(rows, idx, field))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| anyhow::anyhow!("RecordBatch::try_new: {e}"))
}

/// Parses an `OracleNumber`'s exact decimal text (`-12.5`, `0.001`, `42`; never
/// an exponent) into a `Decimal128` value at `scale`, without allocating. A
/// value with more significant fractional digits than `scale`, or more digits
/// than `precision`, is an error: nothing is rounded or truncated.
fn parse_decimal(s: &str, precision: u8, scale: i8) -> Result<i128> {
    let (negative, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let (int_part, frac_part) = digits.split_once('.').unwrap_or((digits, ""));
    if int_part.is_empty() && frac_part.is_empty()
        || !int_part.bytes().chain(frac_part.bytes()).all(|b| b.is_ascii_digit())
    {
        bail!("not a decimal number");
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
    if needed > precision.min(MAX_DECIMAL_PRECISION) as usize {
        bail!("needs {needed} digits, more than the column's precision {precision}");
    }
    let digits = int.bytes().chain(frac.bytes()).map(|d| d - b'0').chain(std::iter::repeat_n(0, scale - frac.len()));
    let v = digits.fold(0i128, |v, d| v * 10 + d as i128);
    Ok(if negative { -v } else { v })
}

fn build_column<R: FetchedRow>(rows: &[R], idx: usize, field: &Field) -> Result<ArrayRef> {
    let name = field.name();
    let mut buf = String::new();
    // Columns read through their text: numbers are rendered into one reused
    // buffer and parsed from it (no allocation per cell).
    macro_rules! from_text {
        ($builder:expr, |$v:ident| $parse:expr) => {{
            let mut b = $builder;
            for row in rows {
                match row.value(idx)?.text(&mut buf) {
                    None => b.append_null(),
                    Some($v) => b.append_value($parse),
                }
            }
            Ok(Arc::new(b.finish()))
        }};
        ($builder:expr, $native:pat => $direct:expr, |$v:ident| $parse:expr) => {{
            let mut b = $builder;
            for row in rows {
                match row.value(idx)? {
                    $native => b.append_value($direct),
                    value => match value.text(&mut buf) {
                        None => b.append_null(),
                        Some($v) => b.append_value($parse),
                    },
                }
            }
            Ok(Arc::new(b.finish()))
        }};
    }
    match field.data_type() {
        DataType::Float32 => from_text!(Float32Builder::with_capacity(rows.len()), Value::F32(f) => f, |v| v
            .parse::<f32>()
            .map_err(|e| anyhow::anyhow!("col {name}: f32 '{v}': {e}"))?),
        DataType::Int32 => from_text!(Int32Builder::with_capacity(rows.len()), |v| v
            .parse::<i32>()
            .map_err(|e| anyhow::anyhow!("col {name}: int32 '{v}': {e}"))?),
        DataType::Int64 => from_text!(Int64Builder::with_capacity(rows.len()), |v| v
            .parse::<i64>()
            .map_err(|e| anyhow::anyhow!("col {name}: int64 '{v}': {e}"))?),
        DataType::Decimal128(precision, scale) => {
            let builder = Decimal128Builder::with_capacity(rows.len())
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| anyhow::anyhow!("col {name}: {e}"))?;
            from_text!(builder, |v| parse_decimal(v, *precision, *scale).map_err(|e| {
                anyhow::anyhow!(
                    "col {name}: NUMBER '{v}' {e} (Decimal128({precision}, {scale})); \
                     declare the column's precision with a CAST in the query, or raise `number_scale`"
                )
            })?)
        }
        DataType::Float64 => from_text!(Float64Builder::with_capacity(rows.len()), Value::F64(f) => f, |v| v
            .parse::<f64>()
            .map_err(|e| anyhow::anyhow!("col {name}: f64 '{v}': {e}"))?),
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.value(idx)? {
                    Value::Bool(v) => b.append_value(v),
                    value => match value.text(&mut buf) {
                        None => b.append_null(),
                        Some("1") | Some("true") | Some("TRUE") => b.append_value(true),
                        Some("0") | Some("false") | Some("FALSE") => b.append_value(false),
                        Some(v) => bail!("col {name}: unexpected bool value '{v}'"),
                    },
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(unit, tz) => {
            macro_rules! timestamps {
                ($builder:ty) => {{
                    let mut b = <$builder>::with_capacity(rows.len()).with_timezone_opt(tz.clone());
                    for row in rows {
                        match row.value(idx)? {
                            Value::Temporal(dt) => b.append_value(
                                epoch_in(dt, *unit).map_err(|e| anyhow::anyhow!("col {name}: timestamp {dt}: {e}"))?,
                            ),
                            Value::Null => b.append_null(),
                            other => bail!("col {name}: expected a timestamp, fetched {other:?}"),
                        }
                    }
                    Ok(Arc::new(b.finish()) as ArrayRef)
                }};
            }
            match unit {
                TimeUnit::Second => timestamps!(TimestampSecondBuilder),
                TimeUnit::Millisecond => timestamps!(TimestampMillisecondBuilder),
                TimeUnit::Microsecond => timestamps!(TimestampMicrosecondBuilder),
                TimeUnit::Nanosecond => timestamps!(TimestampNanosecondBuilder),
            }
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
            for row in rows {
                match row.value(idx)? {
                    Value::Null => b.append_null(),
                    Value::Bytes(bytes) => b.append_value(bytes),
                    Value::Temporal(dt) => bail!("col {name}: unexpected timestamp {dt} in a binary column"),
                    value => match value.text(&mut buf) {
                        Some(s) => b.append_value(s.as_bytes()),
                        None => b.append_null(),
                    },
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let mut b = IntervalMonthDayNanoBuilder::with_capacity(rows.len());
            for row in rows {
                match row.value(idx)?.text(&mut buf) {
                    None => b.append_null(),
                    Some(v) => b.append_value(
                        parse_interval(v).map_err(|e| anyhow::anyhow!("col {name}: interval '{v}': {e}"))?,
                    ),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                match row.value(idx)?.text(&mut buf) {
                    None => b.append_null(),
                    Some(v) => b.append_value(v),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => bail!("col {name}: unsupported arrow type {other:?}"),
    }
}

/// An Oracle interval's `TO_CHAR` text: `+01-02` (YEAR TO MONTH: years and
/// months) or `-03 04:05:06.500000` (DAY TO SECOND), one sign for the whole.
pub fn parse_interval(s: &str) -> Result<IntervalMonthDayNano> {
    let (negative, body) = match s.trim().as_bytes().first() {
        Some(b'-') => (true, &s.trim()[1..]),
        Some(b'+') => (false, &s.trim()[1..]),
        _ => (false, s.trim()),
    };
    let sign = |v: i64| if negative { -v } else { v };
    let int = |t: &str| t.parse::<i64>().map_err(|_| anyhow::anyhow!("not an interval"));
    let overflow = || anyhow::anyhow!("out of range");
    if let Some((days, time)) = body.split_once(' ') {
        let mut hms = time.splitn(3, ':');
        let (h, m, sec) = (hms.next().unwrap_or(""), hms.next().unwrap_or(""), hms.next().unwrap_or(""));
        let (whole, frac) = sec.split_once('.').unwrap_or((sec, ""));
        if frac.len() > 9 || !frac.bytes().all(|b| b.is_ascii_digit()) {
            bail!("not an interval");
        }
        let nanos = ((int(h)? * 60 + int(m)?) * 60 + int(whole)?) * 1_000_000_000
            + if frac.is_empty() { 0 } else { int(&format!("{frac:0<9}"))? };
        Ok(IntervalMonthDayNano::new(0, i32::try_from(sign(int(days)?)).map_err(|_| overflow())?, sign(nanos)))
    } else {
        let (years, months) = body.split_once('-').ok_or_else(|| anyhow::anyhow!("not an interval"))?;
        let months = int(years)?.checked_mul(12).and_then(|y| y.checked_add(int(months).ok()?)).ok_or_else(overflow)?;
        Ok(IntervalMonthDayNano::new(i32::try_from(sign(months)).map_err(|_| overflow())?, 0, 0))
    }
}

/// An interval as the text `TO_YMINTERVAL` (`+Y-M`) or `TO_DSINTERVAL`
/// (`+D HH:MI:SS.FFFFFFFFF`) reads for a target of that type, or ISO-8601
/// for any other column. A value the target type can't hold (months for a
/// DAY TO SECOND column, mixed signs) fails instead of being cut.
fn interval_text(months: i32, days: i32, nanos: i64, target: Option<&DbType>) -> Result<String> {
    match target {
        Some(t) if t == oracledb::DB_TYPE_INTERVAL_YM => {
            if days != 0 || nanos != 0 {
                bail!("an interval with days or time does not fit INTERVAL YEAR TO MONTH");
            }
            let sign = if months < 0 { '-' } else { '+' };
            Ok(format!("{sign}{}-{}", months.unsigned_abs() / 12, months.unsigned_abs() % 12))
        }
        Some(t) if t == oracledb::DB_TYPE_INTERVAL_DS => {
            if months != 0 {
                bail!("an interval with months does not fit INTERVAL DAY TO SECOND");
            }
            if (days < 0 && nanos > 0) || (days > 0 && nanos < 0) {
                bail!("an interval whose days and time differ in sign does not fit INTERVAL DAY TO SECOND");
            }
            let sign = if days < 0 || nanos < 0 { '-' } else { '+' };
            let n = nanos.unsigned_abs();
            let (secs, frac) = (n / 1_000_000_000, n % 1_000_000_000);
            Ok(format!(
                "{sign}{} {:02}:{:02}:{:02}.{frac:09}",
                days.unsigned_abs(),
                secs / 3600,
                secs / 60 % 60,
                secs % 60
            ))
        }
        _ => {
            let sign = if nanos < 0 { "-" } else { "" };
            let n = nanos.unsigned_abs();
            let (secs, frac) = (n / 1_000_000_000, n % 1_000_000_000);
            let frac = if frac == 0 { String::new() } else { format!(".{frac:09}").trim_end_matches('0').to_string() };
            Ok(format!("P{months}M{days}DT{sign}{secs}{frac}S"))
        }
    }
}

// ── Arrow → Oracle bind (destination) ────────────────────────────────────────

/// Whether `field` is a UUID (`FixedSizeBinary(16)` of the `arrow.uuid`
/// extension).
pub fn is_uuid(field: &Field) -> bool {
    field.data_type() == &DataType::FixedSizeBinary(16)
        && field.metadata().get("ARROW:extension:name").map(String::as_str) == Some("arrow.uuid")
}

/// A UUID cell's canonical text (`a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`);
/// `None` for NULL.
pub fn uuid_text(array: &dyn Array, row: usize) -> Result<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let b = array.as_fixed_size_binary_opt().context("array type mismatch")?.value(row);
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    Ok(Some(format!("{}-{}-{}-{}-{}", &hex[..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..])))
}


/// One bind value: text (the default — Oracle converts server-side under the
/// `TO_*` wrappers / implicit char→number) or raw bytes for binary columns.
/// `None` carries a typed NULL.
pub enum Bind {
    Text(Option<String>),
    Bytes(Option<Vec<u8>>),
}

impl Bind {
    /// A `&dyn ToDbValue` pointing at the inner typed `Option`, for `execute_batch`.
    pub fn as_sql(&self) -> &dyn ToDbValue {
        match self {
            Bind::Text(v) => v,
            Bind::Bytes(v) => v,
        }
    }
}

fn is_binary(dt: &DataType) -> bool {
    matches!(dt, DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_))
}

/// How a temporal value is spelled for the target column's `TO_*` wrapper.
fn temporal_text(dt: NaiveDateTime, zoned_value: bool, target: Option<&DbType>) -> String {
    match target {
        // DATE has whole seconds; FMT_DATE has no fraction.
        Some(t) if t == oracledb::DB_TYPE_DATE => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        Some(t) if t == oracledb::DB_TYPE_TIMESTAMP => dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
        // An Arrow timestamp is a UTC instant (or, without a zone, a wall
        // clock taken as UTC).
        Some(t) if is_zoned(t) => format!("{} +00:00", dt.format("%Y-%m-%d %H:%M:%S%.9f")),
        _ => {
            let s = dt.format("%Y-%m-%d %H:%M:%S%.f").to_string();
            if zoned_value { format!("{s} +00:00") } else { s }
        }
    }
}

/// Converts one Arrow cell to a [`Bind`] for a column whose target type is
/// `target` (when the target has the column). Everything but raw bytes is
/// bound as text and converted server-side under the pinned formats.
pub fn cell_to_bind(array: &dyn Array, row: usize, target: Option<&DbType>) -> Result<Bind> {
    if array.is_null(row) {
        return Ok(if is_binary(array.data_type()) { Bind::Bytes(None) } else { Bind::Text(None) });
    }
    macro_rules! prim {
        ($t:ty) => {
            array.as_primitive_opt::<$t>().context("array type mismatch")?.value(row)
        };
    }
    let text = |s: String| Bind::Text(Some(s));
    let b = match array.data_type() {
        DataType::Int8 => text(prim!(Int8Type).to_string()),
        DataType::Int16 => text(prim!(Int16Type).to_string()),
        DataType::Int32 => text(prim!(Int32Type).to_string()),
        DataType::Int64 => text(prim!(Int64Type).to_string()),
        DataType::UInt8 => text(prim!(UInt8Type).to_string()),
        DataType::UInt16 => text(prim!(UInt16Type).to_string()),
        DataType::UInt32 => text(prim!(UInt32Type).to_string()),
        DataType::UInt64 => text(prim!(UInt64Type).to_string()),
        DataType::Float16 => text(prim!(Float16Type).to_string()),
        DataType::Float32 => text(prim!(Float32Type).to_string()),
        DataType::Float64 => text(prim!(Float64Type).to_string()),
        DataType::Decimal128(..) => text(downcast::<Decimal128Array>(array)?.value_as_string(row)),
        DataType::Decimal256(..) => text(downcast::<Decimal256Array>(array)?.value_as_string(row)),
        DataType::Boolean => {
            text(if array.as_boolean_opt().context("array type mismatch")?.value(row) { "1" } else { "0" }.into())
        }
        DataType::Utf8 => text(array.as_string_opt::<i32>().context("array type mismatch")?.value(row).to_string()),
        DataType::LargeUtf8 => text(array.as_string_opt::<i64>().context("array type mismatch")?.value(row).to_string()),
        DataType::Utf8View => text(array.as_string_view_opt().context("array type mismatch")?.value(row).to_string()),
        DataType::Binary => Bind::Bytes(Some(array.as_binary_opt::<i32>().context("array type mismatch")?.value(row).to_vec())),
        DataType::LargeBinary => {
            Bind::Bytes(Some(array.as_binary_opt::<i64>().context("array type mismatch")?.value(row).to_vec()))
        }
        DataType::BinaryView => {
            Bind::Bytes(Some(array.as_binary_view_opt().context("array type mismatch")?.value(row).to_vec()))
        }
        DataType::FixedSizeBinary(_) => {
            Bind::Bytes(Some(array.as_fixed_size_binary_opt().context("array type mismatch")?.value(row).to_vec()))
        }
        DataType::Date32 => {
            let days = prim!(Date32Type);
            let dt = chrono::NaiveDate::default()
                .checked_add_signed(chrono::Duration::days(days as i64))
                .ok_or_else(|| anyhow::anyhow!("date32 out of range: {days}"))?
                .and_hms_opt(0, 0, 0)
                .context("midnight")?;
            text(match target {
                Some(t) if is_one_of(t, &[oracledb::DB_TYPE_DATE, oracledb::DB_TYPE_TIMESTAMP]) || is_zoned(t) => {
                    temporal_text(dt, false, target)
                }
                _ => dt.format("%Y-%m-%d").to_string(),
            })
        }
        DataType::Date64 => {
            let ms = prim!(Date64Type);
            let dt = from_epoch(ms, TimeUnit::Millisecond).ok_or_else(|| anyhow::anyhow!("date64 out of range: {ms}"))?;
            text(temporal_text(dt, false, target))
        }
        DataType::Timestamp(unit, tz) => {
            let v = match unit {
                TimeUnit::Second => prim!(TimestampSecondType),
                TimeUnit::Millisecond => prim!(TimestampMillisecondType),
                TimeUnit::Microsecond => prim!(TimestampMicrosecondType),
                TimeUnit::Nanosecond => prim!(TimestampNanosecondType),
            };
            let dt = from_epoch(v, *unit).ok_or_else(|| anyhow::anyhow!("timestamp out of range: {v}"))?;
            text(temporal_text(dt, tz.is_some(), target))
        }
        DataType::Interval(unit) => {
            let (months, days, nanos) = match unit {
                IntervalUnit::YearMonth => (prim!(IntervalYearMonthType), 0, 0),
                IntervalUnit::DayTime => {
                    let v = prim!(IntervalDayTimeType);
                    (0, v.days, i64::from(v.milliseconds) * 1_000_000)
                }
                IntervalUnit::MonthDayNano => {
                    let v = prim!(IntervalMonthDayNanoType);
                    (v.months, v.days, v.nanoseconds)
                }
            };
            text(interval_text(months, days, nanos, target)?)
        }
        // Oracle has no time-of-day type: the value is bound as text
        // (`HH:MM:SS[.f]`), e.g. for a VARCHAR2 column.
        DataType::Time32(unit) | DataType::Time64(unit) => {
            let v = match array.data_type() {
                DataType::Time32(TimeUnit::Second) => prim!(Time32SecondType) as i64,
                DataType::Time32(_) => prim!(Time32MillisecondType) as i64,
                DataType::Time64(TimeUnit::Microsecond) => prim!(Time64MicrosecondType),
                _ => prim!(Time64NanosecondType),
            };
            let t = time_of_day(v, *unit).ok_or_else(|| anyhow::anyhow!("time of day out of range: {v}"))?;
            text(t.format("%H:%M:%S%.f").to_string())
        }
        other => bail!("the oracle destination does not support Arrow type {other:?}"),
    };
    Ok(b)
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> {
    array.as_any().downcast_ref::<T>().context("array type mismatch")
}

// ── Identifier quoting ────────────────────────────────────────────────────────

/// Quotes an Oracle identifier the way Oracle's own parser resolves a name.
///
/// Oracle folds an *unquoted* identifier to upper-case, so a table created as
/// `events_sink` is stored — and must be referenced — as `EVENTS_SINK`. Config
/// values (`target_table`, `merge_key`) are natural names the user typically
/// writes in lower/mixed case, so a *simple* identifier is folded to upper-case
/// before quoting; that makes `events_sink` reach `EVENTS_SINK` while still
/// quoting the name (safe against reserved words). An identifier that isn't
/// simple (spaces, dots, embedded quotes, leading digit, …) is quoted verbatim,
/// case-preserved — the escape hatch for names that genuinely need it. Names
/// sourced from Oracle's catalog (column descriptions) already arrive
/// upper-cased, so folding is a no-op for them.
pub fn quote_ident(name: &str) -> String {
    if is_simple_ident(name) {
        format!("\"{}\"", name.to_uppercase())
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// Quotes a table name that may be schema-qualified (`sales.orders` →
/// `"SALES"."ORDERS"`): each part as [`quote_ident`] does, a part already in
/// double quotes verbatim (`"Mixed"."my.table"`). A name without a dot outside
/// quotes is one identifier.
pub fn quote_table(name: &str) -> String {
    let mut parts = Vec::new();
    let mut part = String::new();
    let mut quoted = false;
    for c in name.chars() {
        match c {
            '"' => {
                quoted = !quoted;
                part.push(c);
            }
            '.' if !quoted => parts.push(std::mem::take(&mut part)),
            _ => part.push(c),
        }
    }
    parts.push(part);
    parts
        .iter()
        .map(|p| match p.strip_prefix('"').and_then(|q| q.strip_suffix('"')) {
            Some(inner) if !inner.is_empty() => format!("\"{}\"", inner.replace("\"\"", "\"").replace('"', "\"\"")),
            _ => quote_ident(p),
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// The form a config identifier resolves to for *comparison* against Oracle's
/// catalog names (which arrive upper-cased): a simple identifier folded to
/// upper-case, anything else verbatim — i.e. [`quote_ident`] without the quotes.
/// Use this to match a config-supplied name (e.g. a `merge_key`) against the
/// incoming column names so `id` matches the catalog's `ID`.
pub fn fold_ident(name: &str) -> String {
    if is_simple_ident(name) {
        name.to_uppercase()
    } else {
        name.to_string()
    }
}

/// A "simple" Oracle identifier: a leading ASCII letter followed by ASCII
/// letters, digits, or `_ $ #` — the form Oracle accepts unquoted (and folds to
/// upper-case). Anything else must be quoted verbatim to be valid.
fn is_simple_ident(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '$' | '#'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray, TimestampNanosecondArray, TimestampSecondArray, UInt64Array};

    #[test]
    fn type_mapping() {
        let ms = TimeUnit::Millisecond;
        assert_eq!(oracle_to_arrow(oracledb::DB_TYPE_NUMBER, ms), DataType::Decimal128(38, 10));
        assert_eq!(oracle_to_arrow(oracledb::DB_TYPE_BINARY_INTEGER, ms), DataType::Int32);
        assert_eq!(oracle_to_arrow(oracledb::DB_TYPE_TIMESTAMP_TZ, ms), DataType::Timestamp(ms, Some("UTC".into())));
        assert_eq!(oracle_to_arrow(oracledb::DB_TYPE_DATE, TimeUnit::Second), DataType::Timestamp(TimeUnit::Second, None));
        assert_eq!(oracle_to_arrow(oracledb::DB_TYPE_BLOB, ms), DataType::Binary);
        assert_eq!(oracle_to_arrow(oracledb::DB_TYPE_BINARY_DOUBLE, ms), DataType::Float64);
    }

    #[test]
    fn projection_reads_plain_temporals_natively() {
        let cols = vec![
            ColumnDesc { precision: 19, ..ColumnDesc::new("EVENT_ID", oracledb::DB_TYPE_NUMBER) },
            ColumnDesc::new("EVENT_TSTZ", oracledb::DB_TYPE_TIMESTAMP_TZ),
            ColumnDesc::new("EVENT_LTZ", oracledb::DB_TYPE_TIMESTAMP_LTZ),
            ColumnDesc::new("TS", oracledb::DB_TYPE_TIMESTAMP),
            ColumnDesc::new("D", oracledb::DB_TYPE_DATE),
        ];
        let (select, schema) = projected_select(&cols, TimeUnit::Nanosecond, DEFAULT_NUMBER_SCALE, false);
        assert_eq!(
            select,
            "\"EVENT_ID\", SYS_EXTRACT_UTC(\"EVENT_TSTZ\") AS \"EVENT_TSTZ\", \
             SYS_EXTRACT_UTC(CAST(\"EVENT_LTZ\" AS TIMESTAMP(9) WITH TIME ZONE)) AS \"EVENT_LTZ\", \"TS\", \"D\""
        );
        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).data_type(), &DataType::Decimal128(19, 0));
        assert_eq!(schema.field(2).data_type(), &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())));
        assert_eq!(schema.field(3).data_type(), &DataType::Timestamp(TimeUnit::Nanosecond, None));
    }

    #[test]
    fn number_mapping_follows_precision_and_scale() {
        assert_eq!(number_to_arrow(1, 0, 10), DataType::Int32);
        assert_eq!(number_to_arrow(9, 0, 10), DataType::Int32);
        assert_eq!(number_to_arrow(10, 0, 10), DataType::Int64);
        assert_eq!(number_to_arrow(18, 0, 10), DataType::Int64);
        assert_eq!(number_to_arrow(19, 0, 10), DataType::Decimal128(19, 0));
        assert_eq!(number_to_arrow(18, 6, 10), DataType::Decimal128(18, 6));
        assert_eq!(number_to_arrow(38, 38, 10), DataType::Decimal128(38, 38));
        assert_eq!(number_to_arrow(2, 5, 10), DataType::Decimal128(5, 5));
        assert_eq!(number_to_arrow(5, -2, 10), DataType::Decimal128(7, 0));
        assert_eq!(number_to_arrow(126, -127, 10), DataType::Float64);
        assert_eq!(number_to_arrow(0, -127, 10), DataType::Decimal128(38, 10));
        assert_eq!(number_to_arrow(0, -127, 4), DataType::Decimal128(38, 4));
        assert_eq!(number_to_arrow(0, 2, 10), DataType::Decimal128(38, 2));
    }

    #[test]
    fn decimal_text_parses_exactly_or_fails() {
        assert_eq!(parse_decimal("123.45", 18, 6).unwrap(), 123_450_000);
        assert_eq!(parse_decimal("-0.5", 9, 2).unwrap(), -50);
        assert_eq!(parse_decimal("0", 9, 2).unwrap(), 0);
        assert_eq!(parse_decimal("42", 19, 0).unwrap(), 42);
        assert_eq!(parse_decimal(".25", 5, 2).unwrap(), 25);
        assert_eq!(parse_decimal("1.500", 5, 1).unwrap(), 15);
        let max = "9".repeat(38);
        assert_eq!(parse_decimal(&max, 38, 0).unwrap(), max.parse::<i128>().unwrap());
        assert!(parse_decimal("1.234", 9, 2).unwrap_err().to_string().contains("scale 2"));
        assert!(parse_decimal("12345", 4, 0).unwrap_err().to_string().contains("precision 4"));
        assert!(parse_decimal("1e5", 9, 0).is_err());
        assert!(parse_decimal("", 9, 0).is_err());
        assert!(parse_decimal("-", 9, 0).is_err());
    }

    #[test]
    fn numbers_build_typed_columns() {
        let schema = Schema::new(vec![
            Field::new("I", DataType::Int32, true),
            Field::new("L", DataType::Int64, true),
            Field::new("D", DataType::Decimal128(18, 6), true),
        ]);
        let rows = vec![
            vec![Value::Text("7".into()), Value::Text("9000000000".into()), Value::Text("-12.5".into())],
            vec![Value::Null, Value::Null, Value::Null],
        ];
        let batch = rows_to_batch(&rows, &schema).unwrap();
        assert_eq!(batch.column(0).as_primitive::<Int32Type>().value(0), 7);
        assert_eq!(batch.column(1).as_primitive::<Int64Type>().value(0), 9_000_000_000);
        let d = batch.column(2).as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(d.value_as_string(0), "-12.500000");
        assert!(d.is_null(1));
        let bad = vec![vec![Value::Null, Value::Null, Value::Text("0.0000001".into())]];
        assert!(rows_to_batch(&bad, &schema).unwrap_err().to_string().contains("number_scale"));
    }

    #[test]
    fn native_values_build_like_their_text() {
        use std::str::FromStr;
        let n = |s: &str| Value::Number(oracledb::OracleNumber::from_str(s).unwrap());
        let schema = Schema::new(vec![
            Field::new("I", DataType::Int64, true),
            Field::new("D", DataType::Decimal128(38, 10), true),
            Field::new("F", DataType::Float64, true),
            Field::new("G", DataType::Float32, true),
            Field::new("B", DataType::Boolean, true),
            Field::new("S", DataType::Utf8, true),
            Field::new("R", DataType::Binary, true),
        ]);
        let rows = vec![
            vec![n("-42"), n("0.0000000001"), Value::F64(0.1), Value::F32(0.1), Value::Bool(true), n("12.5"), n("7")],
            vec![n("0"), n("-123456789.5"), Value::F32(0.1), Value::F64(1e300), Value::Null, Value::F64(2.5), Value::Null],
        ];
        let batch = rows_to_batch(&rows, &schema).unwrap();
        assert_eq!(batch.column(0).as_primitive::<Int64Type>().values(), &[-42, 0]);
        let d = batch.column(1).as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!((d.value(0), d.value_as_string(1)), (1, "-123456789.5000000000".to_string()));
        // An f32 read into a Float64 goes through its text, as it always did
        // (0.1, not 0.10000000149…); an f64 too large for f32 becomes inf.
        assert_eq!(batch.column(2).as_primitive::<Float64Type>().values(), &[0.1, 0.1]);
        assert_eq!(batch.column(3).as_primitive::<Float32Type>().value(1), f32::INFINITY);
        assert!(batch.column(4).as_boolean().value(0) && batch.column(4).is_null(1));
        assert_eq!(batch.column(5).as_string::<i32>().value(0), "12.5");
        assert_eq!(batch.column(5).as_string::<i32>().value(1), "2.5");
        assert_eq!(batch.column(6).as_binary::<i32>().value(0), b"7");
    }

    #[test]
    fn temporals_build_in_every_unit() {
        let dt = NaiveDateTime::parse_from_str("2024-01-15 12:30:45.123456789", "%Y-%m-%d %H:%M:%S%.f").unwrap();
        let rows = vec![vec![Value::Temporal(dt)], vec![Value::Null]];
        let col = |unit| {
            let schema = Schema::new(vec![Field::new("TS", DataType::Timestamp(unit, None), true)]);
            rows_to_batch(&rows, &schema).unwrap().column(0).clone()
        };
        assert_eq!(col(TimeUnit::Nanosecond).as_primitive::<TimestampNanosecondType>().value(0), 1_705_321_845_123_456_789);
        assert_eq!(col(TimeUnit::Millisecond).as_primitive::<TimestampMillisecondType>().value(0), 1_705_321_845_123);
        assert_eq!(col(TimeUnit::Second).as_primitive::<TimestampSecondType>().value(0), 1_705_321_845);
        assert!(col(TimeUnit::Second).is_null(1));
    }

    #[test]
    fn temporal_watermark_keeps_the_typed_expr_format() {
        let dt = NaiveDateTime::parse_from_str("2024-01-15 12:30:45.5", "%Y-%m-%d %H:%M:%S%.f").unwrap();
        let wm = |t| Value::Temporal(dt).watermark(Some(t)).unwrap();
        assert_eq!(wm(oracledb::DB_TYPE_DATE), "2024-01-15 12:30:45");
        assert_eq!(wm(oracledb::DB_TYPE_TIMESTAMP), "2024-01-15 12:30:45.500000000");
        assert_eq!(wm(oracledb::DB_TYPE_TIMESTAMP_TZ), "2024-01-15 12:30:45.500000000 +00:00");
        assert_eq!(Value::Text("42".into()).watermark(Some(oracledb::DB_TYPE_NUMBER)).as_deref(), Some("42"));
        assert_eq!(Value::Null.watermark(None), None);
    }

    #[test]
    fn typed_expr_wraps_temporal() {
        assert_eq!(typed_expr(":1", oracledb::DB_TYPE_DATE), "TO_DATE(:1, 'YYYY-MM-DD HH24:MI:SS')");
        assert_eq!(typed_expr(":1", oracledb::DB_TYPE_TIMESTAMP), "TO_TIMESTAMP(:1, 'YYYY-MM-DD HH24:MI:SS.FF')");
        assert_eq!(typed_expr(":1", oracledb::DB_TYPE_NUMBER), ":1");
    }

    #[test]
    fn intervals_read_from_to_char_and_bound_for_their_target() {
        let i = IntervalMonthDayNano::new;
        assert_eq!(parse_interval("+01-02").unwrap(), i(14, 0, 0));
        assert_eq!(parse_interval("-01-02").unwrap(), i(-14, 0, 0));
        assert_eq!(parse_interval("+03 04:05:06.500000").unwrap(), i(0, 3, 14_706_500_000_000));
        assert_eq!(parse_interval("-03 04:05:06.000001").unwrap(), i(0, -3, -14_706_000_001_000));
        assert_eq!(parse_interval("+00 00:00:00").unwrap(), i(0, 0, 0));
        assert!(parse_interval("3 days").is_err());
        let (ym, ds) = (Some(oracledb::DB_TYPE_INTERVAL_YM), Some(oracledb::DB_TYPE_INTERVAL_DS));
        assert_eq!(interval_text(-14, 0, 0, ym).unwrap(), "-1-2");
        assert_eq!(interval_text(0, 3, 14_706_500_000_000, ds).unwrap(), "+3 04:05:06.500000000");
        assert_eq!(interval_text(0, -3, -1_000, ds).unwrap(), "-3 00:00:00.000001000");
        assert!(interval_text(1, 0, 0, ds).is_err(), "months don't fit DAY TO SECOND");
        assert!(interval_text(0, 1, 0, ym).is_err(), "days don't fit YEAR TO MONTH");
        assert!(interval_text(0, 1, -5, ds).is_err(), "mixed signs");
        assert_eq!(interval_text(14, -3, 5_500_000_000, None).unwrap(), "P14M-3DT5.5S");
        assert_eq!(typed_expr(":1", oracledb::DB_TYPE_INTERVAL_DS), "TO_DSINTERVAL(:1)");
        let cols = vec![ColumnDesc::new("D", oracledb::DB_TYPE_INTERVAL_DS)];
        let (select, schema) = projected_select(&cols, TimeUnit::Millisecond, DEFAULT_NUMBER_SCALE, false);
        assert_eq!(select, "TO_CHAR(\"D\") AS \"D\"");
        assert_eq!(schema.field(0).data_type(), &DataType::Interval(IntervalUnit::MonthDayNano));
        let (_, schema) = projected_select(&cols, TimeUnit::Millisecond, DEFAULT_NUMBER_SCALE, true);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn cell_to_bind_binds_as_text() {
        let ids = Int64Array::from(vec![Some(7), None]);
        assert!(matches!(cell_to_bind(&ids, 0, None).unwrap(), Bind::Text(Some(s)) if s == "7"));
        assert!(matches!(cell_to_bind(&ids, 1, None).unwrap(), Bind::Text(None)));
        let names = StringArray::from(vec![Some("ab")]);
        assert!(matches!(cell_to_bind(&names, 0, None).unwrap(), Bind::Text(Some(s)) if s == "ab"));
        let big = UInt64Array::from(vec![u64::MAX]);
        assert!(matches!(cell_to_bind(&big, 0, None).unwrap(), Bind::Text(Some(s)) if s == "18446744073709551615"));
        let d = Decimal128Array::from(vec![-12345]).with_precision_and_scale(9, 2).unwrap();
        assert!(matches!(cell_to_bind(&d, 0, None).unwrap(), Bind::Text(Some(s)) if s == "-123.45"));
    }

    #[test]
    fn timestamps_bind_in_the_target_column_format() {
        let ts = TimestampNanosecondArray::from(vec![1_705_321_845_123_456_789]).with_timezone("UTC");
        let bind = |t| match cell_to_bind(&ts, 0, t).unwrap() {
            Bind::Text(Some(s)) => s,
            _ => unreachable!(),
        };
        assert_eq!(bind(Some(oracledb::DB_TYPE_DATE)), "2024-01-15 12:30:45");
        assert_eq!(bind(Some(oracledb::DB_TYPE_TIMESTAMP)), "2024-01-15 12:30:45.123456789");
        assert_eq!(bind(Some(oracledb::DB_TYPE_TIMESTAMP_TZ)), "2024-01-15 12:30:45.123456789 +00:00");
        assert_eq!(bind(None), "2024-01-15 12:30:45.123456789 +00:00");
        let secs = TimestampSecondArray::from(vec![1_705_321_845]);
        assert!(matches!(cell_to_bind(&secs, 0, Some(oracledb::DB_TYPE_TIMESTAMP)).unwrap(),
            Bind::Text(Some(s)) if s == "2024-01-15 12:30:45.000000000"));
    }

    #[test]
    fn quote_table_quotes_each_part() {
        assert_eq!(quote_table("events_sink"), "\"EVENTS_SINK\"");
        assert_eq!(quote_table("sales.orders"), "\"SALES\".\"ORDERS\"");
        assert_eq!(quote_table("\"Sales\".orders"), "\"Sales\".\"ORDERS\"");
        assert_eq!(quote_table("\"my.table\""), "\"my.table\"");
        assert_eq!(quote_table("weird name"), "\"weird name\"");
    }

    #[test]
    fn quote_ident_escapes() {
        // Not a simple identifier (embedded quote) → quoted verbatim, escaped.
        assert_eq!(quote_ident("A\"B"), "\"A\"\"B\"");
    }

    #[test]
    fn quote_ident_folds_simple_names_to_upper() {
        // Config-supplied natural names reach Oracle's upper-cased catalog names.
        assert_eq!(quote_ident("events_sink"), "\"EVENTS_SINK\"");
        assert_eq!(quote_ident("id"), "\"ID\"");
        assert_eq!(quote_ident("MixedCase"), "\"MIXEDCASE\"");
        // Already upper-cased catalog names are unchanged.
        assert_eq!(quote_ident("EVENT_ID"), "\"EVENT_ID\"");
    }

    #[test]
    fn quote_ident_preserves_non_simple_names() {
        // A leading digit or a space is not a simple identifier → verbatim case.
        assert_eq!(quote_ident("1table"), "\"1table\"");
        assert_eq!(quote_ident("weird name"), "\"weird name\"");
    }
}
