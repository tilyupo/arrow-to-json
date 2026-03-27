#![deny(clippy::all)]

use arrow_array::types::{
  Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
  Array, BinaryArray, BooleanArray, DictionaryArray, FixedSizeListArray, LargeBinaryArray,
  LargeListArray, LargeStringArray, ListArray, MapArray, RecordBatch, StringArray, StructArray,
};
use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_schema::{DataType, Field};
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;
use serde_json::{Map, Value};

macro_rules! num_val {
  ($arr:expr, $array_type:ty, $row:expr) => {{
    let a = $arr.as_any().downcast_ref::<$array_type>().unwrap();
    Value::Number(a.value($row).into())
  }};
}

fn array_value_at(arr: &dyn Array, row: usize) -> Value {
  if arr.is_null(row) {
    return Value::Null;
  }
  match arr.data_type() {
    DataType::Boolean => {
      let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
      Value::Bool(a.value(row))
    }
    DataType::Int8 => num_val!(arr, arrow_array::Int8Array, row),
    DataType::Int16 => num_val!(arr, arrow_array::Int16Array, row),
    DataType::Int32 => num_val!(arr, arrow_array::Int32Array, row),
    DataType::Int64 => {
      let a = arr.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
      let v = a.value(row);
      if v.unsigned_abs() <= (1u64 << 53) {
        Value::Number(serde_json::Number::from(v))
      } else {
        Value::String(v.to_string())
      }
    }
    DataType::UInt8 => num_val!(arr, arrow_array::UInt8Array, row),
    DataType::UInt16 => num_val!(arr, arrow_array::UInt16Array, row),
    DataType::UInt32 => num_val!(arr, arrow_array::UInt32Array, row),
    DataType::UInt64 => {
      let a = arr
        .as_any()
        .downcast_ref::<arrow_array::UInt64Array>()
        .unwrap();
      let v = a.value(row);
      if v <= (1u64 << 53) {
        Value::Number(serde_json::Number::from(v))
      } else {
        Value::String(v.to_string())
      }
    }
    DataType::Float16 => {
      let a = arr
        .as_any()
        .downcast_ref::<arrow_array::Float16Array>()
        .unwrap();
      match serde_json::Number::from_f64(a.value(row).to_f64()) {
        Some(n) => Value::Number(n),
        None => Value::Null,
      }
    }
    DataType::Float32 => {
      let a = arr
        .as_any()
        .downcast_ref::<arrow_array::Float32Array>()
        .unwrap();
      match serde_json::Number::from_f64(a.value(row) as f64) {
        Some(n) => Value::Number(n),
        None => Value::Null,
      }
    }
    DataType::Float64 => {
      let a = arr
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .unwrap();
      match serde_json::Number::from_f64(a.value(row)) {
        Some(n) => Value::Number(n),
        None => Value::Null,
      }
    }
    DataType::Utf8 => {
      let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
      Value::String(a.value(row).to_owned())
    }
    DataType::LargeUtf8 => {
      let a = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
      Value::String(a.value(row).to_owned())
    }
    DataType::Binary => {
      let a = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
      Value::Array(
        a.value(row)
          .iter()
          .map(|b| Value::Number((*b).into()))
          .collect(),
      )
    }
    DataType::LargeBinary => {
      let a = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
      Value::Array(
        a.value(row)
          .iter()
          .map(|b| Value::Number((*b).into()))
          .collect(),
      )
    }
    DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
      list_value(arr, row)
    }
    DataType::Struct(fields) => struct_value(arr, fields, row),
    DataType::Map(entry_field, _sorted) => map_value(arr, entry_field, row),
    DataType::Dictionary(_, _) => dict_value(arr, row),
    // Temporal types: cast to Utf8 and emit the string representation
    DataType::Timestamp(_, _)
    | DataType::Date32
    | DataType::Date64
    | DataType::Time32(_)
    | DataType::Time64(_)
    | DataType::Duration(_)
    | DataType::Interval(_) => {
      match arrow_cast::cast(arr, &DataType::Utf8) {
        Ok(casted) => {
          let sa = casted.as_any().downcast_ref::<StringArray>().unwrap();
          if sa.is_null(row) {
            Value::Null
          } else {
            Value::String(sa.value(row).to_owned())
          }
        }
        Err(_) => Value::String(format!("<unsupported:{:?}>", arr.data_type())),
      }
    }
    _ => Value::String(format!("<unsupported:{:?}>", arr.data_type())),
  }
}

fn list_value(arr: &dyn Array, row: usize) -> Value {
  if let Some(a) = arr.as_any().downcast_ref::<ListArray>() {
    let values = a.values();
    let start = a.value_offsets()[row] as usize;
    let end = a.value_offsets()[row + 1] as usize;
    Value::Array(
      (start..end)
        .map(|i| array_value_at(values.as_ref(), i))
        .collect(),
    )
  } else if let Some(a) = arr.as_any().downcast_ref::<LargeListArray>() {
    let values = a.values();
    let start = a.value_offsets()[row] as usize;
    let end = a.value_offsets()[row + 1] as usize;
    Value::Array(
      (start..end)
        .map(|i| array_value_at(values.as_ref(), i))
        .collect(),
    )
  } else if let Some(a) = arr.as_any().downcast_ref::<FixedSizeListArray>() {
    let values = a.values();
    let size = a.value_length() as usize;
    let start = row * size;
    let end = start + size;
    Value::Array(
      (start..end)
        .map(|i| array_value_at(values.as_ref(), i))
        .collect(),
    )
  } else {
    Value::Null
  }
}

fn struct_value(arr: &dyn Array, fields: &arrow_schema::Fields, row: usize) -> Value {
  let sa = arr.as_any().downcast_ref::<StructArray>().unwrap();
  let mut obj = Map::with_capacity(fields.len());
  for (i, field) in fields.iter().enumerate() {
    let val = array_value_at(sa.column(i).as_ref(), row);
    obj.insert(field.name().clone(), val);
  }
  Value::Object(obj)
}

fn map_value(arr: &dyn Array, entry_field: &Field, row: usize) -> Value {
  let ma = arr.as_any().downcast_ref::<MapArray>().unwrap();
  let start = ma.value_offsets()[row] as usize;
  let end = ma.value_offsets()[row + 1] as usize;
  if start == end {
    return Value::Null;
  }

  let entries = ma.entries();
  let struct_fields = match entry_field.data_type() {
    DataType::Struct(f) => f,
    _ => return Value::Null,
  };

  let key_col = entries.column(0);
  let val_col = entries.column(1);
  let key_is_utf8 = matches!(
    struct_fields[0].data_type(),
    DataType::Utf8 | DataType::LargeUtf8
  );

  if key_is_utf8 {
    let mut obj = Map::with_capacity(end - start);
    for i in start..end {
      let key = if let Some(a) = key_col.as_any().downcast_ref::<StringArray>() {
        a.value(i).to_owned()
      } else if let Some(a) = key_col.as_any().downcast_ref::<LargeStringArray>() {
        a.value(i).to_owned()
      } else {
        continue;
      };
      obj.insert(key, array_value_at(val_col.as_ref(), i));
    }
    Value::Object(obj)
  } else {
    Value::Array(
      (start..end)
        .map(|i| {
          let mut entry = Map::with_capacity(2);
          entry.insert("key".into(), array_value_at(key_col.as_ref(), i));
          entry.insert("value".into(), array_value_at(val_col.as_ref(), i));
          Value::Object(entry)
        })
        .collect(),
    )
  }
}

fn dict_value(arr: &dyn Array, row: usize) -> Value {
  macro_rules! try_dict {
    ($arr:expr, $key_ty:ty, $row:expr) => {
      if let Some(da) = $arr
        .as_any()
        .downcast_ref::<DictionaryArray<$key_ty>>()
      {
        let key = da.keys().value($row) as usize;
        return array_value_at(da.values().as_ref(), key);
      }
    };
  }

  try_dict!(arr, Int8Type, row);
  try_dict!(arr, Int16Type, row);
  try_dict!(arr, Int32Type, row);
  try_dict!(arr, Int64Type, row);
  try_dict!(arr, UInt8Type, row);
  try_dict!(arr, UInt16Type, row);
  try_dict!(arr, UInt32Type, row);
  try_dict!(arr, UInt64Type, row);

  Value::Null
}

fn record_batch_to_rows(batch: &RecordBatch) -> Vec<Value> {
  let schema = batch.schema();
  let num_rows = batch.num_rows();
  let columns: Vec<(&str, &dyn Array)> = schema
    .fields()
    .iter()
    .enumerate()
    .map(|(i, f)| (f.name().as_str(), batch.column(i).as_ref()))
    .collect();

  let mut rows = Vec::with_capacity(num_rows);
  for row in 0..num_rows {
    let mut obj = Map::with_capacity(columns.len());
    for &(name, col) in &columns {
      let val = array_value_at(col, row);
      if val != Value::Null {
        obj.insert(name.to_owned(), val);
      }
    }
    rows.push(Value::Object(obj));
  }
  rows
}

/// Arrow IPC magic bytes for the file format
const ARROW_FILE_MAGIC: &[u8] = b"ARROW1";

fn is_ipc_file(data: &[u8]) -> bool {
  data.len() >= 8 && data[..6] == *ARROW_FILE_MAGIC
}

/// Converts Arrow IPC bytes to a JSON string.
///
/// Accepts both Arrow IPC file format and streaming format.
/// Returns a JSON array string where each element is a row object
/// with column names as keys.
///
/// - Null values are omitted from output objects.
/// - `Map<Utf8, *>` columns are emitted as JSON objects.
/// - `Int64`/`UInt64` values exceeding 2^53 are emitted as strings.
/// - Temporal types are cast to their string representation.
#[napi]
pub fn arrow_ipc_to_json(data: Buffer) -> napi::Result<String> {
  let bytes = data.as_ref();
  let mut all_rows: Vec<Value> = Vec::new();

  if is_ipc_file(bytes) {
    let cursor = std::io::Cursor::new(bytes);
    let reader = FileReader::try_new(cursor, None)
      .map_err(|e| napi::Error::from_reason(format!("Failed to read Arrow IPC file: {e}")))?;
    for batch_result in reader {
      let batch = batch_result
        .map_err(|e| napi::Error::from_reason(format!("Failed to read batch: {e}")))?;
      all_rows.extend(record_batch_to_rows(&batch));
    }
  } else {
    let cursor = std::io::Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None)
      .map_err(|e| napi::Error::from_reason(format!("Failed to read Arrow IPC stream: {e}")))?;
    for batch_result in reader {
      let batch = batch_result
        .map_err(|e| napi::Error::from_reason(format!("Failed to read batch: {e}")))?;
      all_rows.extend(record_batch_to_rows(&batch));
    }
  }

  serde_json::to_string(&all_rows)
    .map_err(|e| napi::Error::from_reason(format!("JSON serialization failed: {e}")))
}
