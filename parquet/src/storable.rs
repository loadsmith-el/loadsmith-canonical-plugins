//! Arrow types Parquet cannot store, rewritten as exact ones it can.
//!
//! `Interval(MonthDayNano)` (a Postgres or Oracle `INTERVAL`): Parquet's own
//! INTERVAL is unsigned months/days/milliseconds, and the Arrow writer refuses
//! the type, so each value becomes a
//! `struct { months: int32, days: int32, nanoseconds: int64 }` — every part
//! kept, signed, queryable (`col.days`). Inside lists and structs too.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::IntervalMonthDayNanoType;
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, LargeListArray, ListArray, RecordBatch, StructArray};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Fields, IntervalUnit, Schema};

/// The struct an `Interval(MonthDayNano)` is stored as.
fn interval_fields() -> Fields {
    Fields::from(vec![
        Field::new("months", DataType::Int32, false),
        Field::new("days", DataType::Int32, false),
        Field::new("nanoseconds", DataType::Int64, false),
    ])
}

/// Whether `dt` holds a type this module rewrites.
fn needs_rewrite(dt: &DataType) -> bool {
    match dt {
        DataType::Interval(IntervalUnit::MonthDayNano) => true,
        DataType::List(f) | DataType::LargeList(f) => needs_rewrite(f.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| needs_rewrite(f.data_type())),
        _ => false,
    }
}

/// `field` with its type rewritten.
fn storable_field(field: &FieldRef) -> FieldRef {
    if !needs_rewrite(field.data_type()) {
        return field.clone();
    }
    let dt = match field.data_type() {
        DataType::List(f) => DataType::List(storable_field(f)),
        DataType::LargeList(f) => DataType::LargeList(storable_field(f)),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(storable_field).collect()),
        _ => DataType::Struct(interval_fields()),
    };
    Arc::new(field.as_ref().clone().with_data_type(dt))
}

/// `array` (of `field`'s type) rewritten to [`storable_field`]'s type.
fn storable_array(array: &ArrayRef, field: &FieldRef) -> Result<ArrayRef, ArrowError> {
    if !needs_rewrite(field.data_type()) {
        return Ok(array.clone());
    }
    Ok(match field.data_type() {
        DataType::Interval(_) => {
            let a = array.as_primitive::<IntervalMonthDayNanoType>();
            let months = Int32Array::from_iter_values(a.values().iter().map(|v| v.months));
            let days = Int32Array::from_iter_values(a.values().iter().map(|v| v.days));
            let nanos = Int64Array::from_iter_values(a.values().iter().map(|v| v.nanoseconds));
            Arc::new(StructArray::try_new(
                interval_fields(),
                vec![Arc::new(months), Arc::new(days), Arc::new(nanos)],
                a.nulls().cloned(),
            )?)
        }
        DataType::List(item) => {
            let a = array.as_list::<i32>();
            let values = storable_array(a.values(), item)?;
            Arc::new(ListArray::try_new(storable_field(item), a.offsets().clone(), values, a.nulls().cloned())?)
        }
        DataType::LargeList(item) => {
            let a = array.as_list::<i64>();
            let values = storable_array(a.values(), item)?;
            Arc::new(LargeListArray::try_new(storable_field(item), a.offsets().clone(), values, a.nulls().cloned())?)
        }
        DataType::Struct(fields) => {
            let a = array.as_struct();
            let columns = a.columns().iter().zip(fields.iter()).map(|(c, f)| storable_array(c, f)).collect::<Result<_, _>>()?;
            Arc::new(StructArray::try_new(fields.iter().map(storable_field).collect(), columns, a.nulls().cloned())?)
        }
        _ => array.clone(),
    })
}

/// `batch` with the types Parquet can't store rewritten; the same batch when
/// it has none.
pub fn storable(batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
    let schema = batch.schema();
    if !schema.fields().iter().any(|f| needs_rewrite(f.data_type())) {
        return Ok(batch);
    }
    let fields: Vec<FieldRef> = schema.fields().iter().map(storable_field).collect();
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields().iter())
        .map(|(c, f)| storable_array(c, f))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())), columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::IntervalMonthDayNano;
    use arrow_array::IntervalMonthDayNanoArray;

    #[test]
    fn intervals_become_exact_structs_at_any_depth() {
        let iv = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(14, -3, 5_000_000_001)),
            None,
        ]);
        let list = ListArray::from_iter_primitive::<IntervalMonthDayNanoType, _, _>(vec![
            Some(vec![Some(IntervalMonthDayNano::new(1, 2, 3))]),
            None,
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("iv", iv.data_type().clone(), true),
            Field::new("ivs", list.data_type().clone(), true),
            Field::new("n", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(iv), Arc::new(list), Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let out = storable(batch).unwrap();
        let s = out.column(0).as_struct();
        assert_eq!(s.column(0).as_primitive::<arrow_array::types::Int32Type>().value(0), 14);
        assert_eq!(s.column(1).as_primitive::<arrow_array::types::Int32Type>().value(0), -3);
        assert_eq!(s.column(2).as_primitive::<arrow_array::types::Int64Type>().value(0), 5_000_000_001);
        assert!(s.is_null(1));
        let DataType::List(item) = out.schema().field(1).data_type().clone() else { panic!() };
        assert_eq!(item.data_type(), &DataType::Struct(interval_fields()));
        assert_eq!(out.schema().field(2).data_type(), &DataType::Int64);
    }

    #[test]
    fn a_batch_without_intervals_is_untouched() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        assert_eq!(storable(batch.clone()).unwrap(), batch);
    }

    #[test]
    fn a_uuid_field_is_parquets_uuid_type() {
        let field = Field::new("id", DataType::FixedSizeBinary(16), true).with_metadata(
            std::collections::HashMap::from([("ARROW:extension:name".to_string(), "arrow.uuid".to_string())]),
        );
        let parquet = parquet::arrow::ArrowSchemaConverter::new().convert(&Schema::new(vec![field])).unwrap();
        assert_eq!(parquet.column(0).logical_type_ref(), Some(&parquet::basic::LogicalType::Uuid));
    }
}
