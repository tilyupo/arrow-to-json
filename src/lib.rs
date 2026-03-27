#![deny(clippy::all)]

use std::fmt::Write as FmtWrite;

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

// ---------------------------------------------------------------------------
// JSON string escaping
// ---------------------------------------------------------------------------

fn write_json_str(buf: &mut String, s: &str) {
  buf.push('"');
  let bytes = s.as_bytes();
  let mut start = 0;
  for (i, &b) in bytes.iter().enumerate() {
    let esc: &str = match b {
      b'"' => "\\\"",
      b'\\' => "\\\\",
      b'\n' => "\\n",
      b'\r' => "\\r",
      b'\t' => "\\t",
      0x08 => "\\b",
      0x0c => "\\f",
      0x00..=0x1f => {
        buf.push_str(&s[start..i]);
        let _ = write!(buf, "\\u{:04x}", b);
        start = i + 1;
        continue;
      }
      _ => continue,
    };
    buf.push_str(&s[start..i]);
    buf.push_str(esc);
    start = i + 1;
  }
  buf.push_str(&s[start..]);
  buf.push('"');
}

// ---------------------------------------------------------------------------
// Pre-downcast column writer — resolves Arrow type ONCE per column per batch,
// eliminating per-cell dynamic dispatch + downcast_ref in the hot loop.
// ---------------------------------------------------------------------------

enum ColWriter<'a> {
  Bool(&'a BooleanArray),
  I8(&'a arrow_array::Int8Array),
  I16(&'a arrow_array::Int16Array),
  I32(&'a arrow_array::Int32Array),
  I64(&'a arrow_array::Int64Array),
  U8(&'a arrow_array::UInt8Array),
  U16(&'a arrow_array::UInt16Array),
  U32(&'a arrow_array::UInt32Array),
  U64(&'a arrow_array::UInt64Array),
  F32(&'a arrow_array::Float32Array),
  F64(&'a arrow_array::Float64Array),
  Utf8(&'a StringArray),
  LargeUtf8(&'a LargeStringArray),
  MapUtf8Utf8 {
    ma: &'a MapArray,
    keys: &'a StringArray,
    vals: &'a StringArray,
  },
  ListI64 {
    list: &'a ListArray,
    values: &'a arrow_array::Int64Array,
  },
  Generic(&'a dyn Array),
}

fn resolve_writer(col: &dyn Array) -> ColWriter<'_> {
  match col.data_type() {
    DataType::Boolean => ColWriter::Bool(col.as_any().downcast_ref().unwrap()),
    DataType::Int8 => ColWriter::I8(col.as_any().downcast_ref().unwrap()),
    DataType::Int16 => ColWriter::I16(col.as_any().downcast_ref().unwrap()),
    DataType::Int32 => ColWriter::I32(col.as_any().downcast_ref().unwrap()),
    DataType::Int64 => ColWriter::I64(col.as_any().downcast_ref().unwrap()),
    DataType::UInt8 => ColWriter::U8(col.as_any().downcast_ref().unwrap()),
    DataType::UInt16 => ColWriter::U16(col.as_any().downcast_ref().unwrap()),
    DataType::UInt32 => ColWriter::U32(col.as_any().downcast_ref().unwrap()),
    DataType::UInt64 => ColWriter::U64(col.as_any().downcast_ref().unwrap()),
    DataType::Float32 => ColWriter::F32(col.as_any().downcast_ref().unwrap()),
    DataType::Float64 => ColWriter::F64(col.as_any().downcast_ref().unwrap()),
    DataType::Utf8 => ColWriter::Utf8(col.as_any().downcast_ref().unwrap()),
    DataType::LargeUtf8 => ColWriter::LargeUtf8(col.as_any().downcast_ref().unwrap()),
    DataType::Map(entry_field, _) => try_map_utf8(col, entry_field),
    DataType::List(inner) if matches!(inner.data_type(), DataType::Int64) => {
      let list = col.as_any().downcast_ref::<ListArray>().unwrap();
      ColWriter::ListI64 {
        list,
        values: list
          .values()
          .as_any()
          .downcast_ref::<arrow_array::Int64Array>()
          .unwrap(),
      }
    }
    _ => ColWriter::Generic(col),
  }
}

fn try_map_utf8<'a>(col: &'a dyn Array, entry_field: &Field) -> ColWriter<'a> {
  if let DataType::Struct(fields) = entry_field.data_type() {
    if fields.len() == 2
      && matches!(fields[0].data_type(), DataType::Utf8)
      && matches!(fields[1].data_type(), DataType::Utf8)
    {
      let ma = col.as_any().downcast_ref::<MapArray>().unwrap();
      let entries = ma.entries();
      if let (Some(keys), Some(vals)) = (
        entries
          .column(0)
          .as_any()
          .downcast_ref::<StringArray>(),
        entries
          .column(1)
          .as_any()
          .downcast_ref::<StringArray>(),
      ) {
        return ColWriter::MapUtf8Utf8 { ma, keys, vals };
      }
    }
  }
  ColWriter::Generic(col)
}

// ---------------------------------------------------------------------------
// Write a single column value at a given row directly into the buffer.
// Called from the hot loop — the enum match here replaces per-cell
// data_type() matching + downcast_ref.
// ---------------------------------------------------------------------------

fn write_col(writer: &ColWriter, row: usize, buf: &mut String) {
  match writer {
    ColWriter::Bool(a) => {
      buf.push_str(if a.value(row) { "true" } else { "false" });
    }
    ColWriter::I8(a) => {
      let mut b = itoa::Buffer::new();
      buf.push_str(b.format(a.value(row)));
    }
    ColWriter::I16(a) => {
      let mut b = itoa::Buffer::new();
      buf.push_str(b.format(a.value(row)));
    }
    ColWriter::I32(a) => {
      let mut b = itoa::Buffer::new();
      buf.push_str(b.format(a.value(row)));
    }
    ColWriter::I64(a) => write_i64_val(a.value(row), buf),
    ColWriter::U8(a) => {
      let mut b = itoa::Buffer::new();
      buf.push_str(b.format(a.value(row)));
    }
    ColWriter::U16(a) => {
      let mut b = itoa::Buffer::new();
      buf.push_str(b.format(a.value(row)));
    }
    ColWriter::U32(a) => {
      let mut b = itoa::Buffer::new();
      buf.push_str(b.format(a.value(row)));
    }
    ColWriter::U64(a) => write_u64_val(a.value(row), buf),
    ColWriter::F32(a) => write_f64_val(a.value(row) as f64, buf),
    ColWriter::F64(a) => write_f64_val(a.value(row), buf),
    ColWriter::Utf8(a) => write_json_str(buf, a.value(row)),
    ColWriter::LargeUtf8(a) => write_json_str(buf, a.value(row)),
    ColWriter::MapUtf8Utf8 { ma, keys, vals } => {
      write_map_utf8_utf8(ma, keys, vals, row, buf);
    }
    ColWriter::ListI64 { list, values } => {
      write_list_i64(list, values, row, buf);
    }
    ColWriter::Generic(a) => write_value(*a, row, buf),
  }
}

#[inline]
fn write_i64_val(v: i64, buf: &mut String) {
  let mut b = itoa::Buffer::new();
  if v.unsigned_abs() <= (1u64 << 53) {
    buf.push_str(b.format(v));
  } else {
    buf.push('"');
    buf.push_str(b.format(v));
    buf.push('"');
  }
}

#[inline]
fn write_u64_val(v: u64, buf: &mut String) {
  let mut b = itoa::Buffer::new();
  if v <= (1u64 << 53) {
    buf.push_str(b.format(v));
  } else {
    buf.push('"');
    buf.push_str(b.format(v));
    buf.push('"');
  }
}

#[inline]
fn write_f64_val(v: f64, buf: &mut String) {
  if v.is_finite() {
    let mut b = ryu::Buffer::new();
    buf.push_str(b.format_finite(v));
  } else {
    buf.push_str("null");
  }
}

fn write_map_utf8_utf8(
  ma: &MapArray,
  keys: &StringArray,
  vals: &StringArray,
  row: usize,
  buf: &mut String,
) {
  let start = ma.value_offsets()[row] as usize;
  let end = ma.value_offsets()[row + 1] as usize;
  if start == end {
    buf.push_str("null");
    return;
  }
  buf.push('{');
  for i in start..end {
    if i > start {
      buf.push(',');
    }
    if keys.is_null(i) {
      continue;
    }
    write_json_str(buf, keys.value(i));
    buf.push(':');
    if vals.is_null(i) {
      buf.push_str("null");
    } else {
      write_json_str(buf, vals.value(i));
    }
  }
  buf.push('}');
}

fn write_list_i64(
  list: &ListArray,
  values: &arrow_array::Int64Array,
  row: usize,
  buf: &mut String,
) {
  let start = list.value_offsets()[row] as usize;
  let end = list.value_offsets()[row + 1] as usize;
  buf.push('[');
  for i in start..end {
    if i > start {
      buf.push(',');
    }
    if values.is_null(i) {
      buf.push_str("null");
    } else {
      write_i64_val(values.value(i), buf);
    }
  }
  buf.push(']');
}

// ---------------------------------------------------------------------------
// should_skip — top-level decision to omit a field from the row object
// ---------------------------------------------------------------------------

fn should_skip_writer(writer: &ColWriter, row: usize) -> bool {
  match writer {
    ColWriter::MapUtf8Utf8 { ma, .. } => {
      if ma.is_null(row) {
        return true;
      }
      let start = ma.value_offsets()[row] as usize;
      let end = ma.value_offsets()[row + 1] as usize;
      start == end
    }
    ColWriter::Generic(a) => {
      if a.is_null(row) {
        return true;
      }
      if let DataType::Map(_, _) = a.data_type() {
        let ma = a.as_any().downcast_ref::<MapArray>().unwrap();
        let start = ma.value_offsets()[row] as usize;
        let end = ma.value_offsets()[row + 1] as usize;
        start == end
      } else {
        false
      }
    }
    _ => false,
  }
}

// ---------------------------------------------------------------------------
// Generic write_value — fallback for column types not in ColWriter enum
// (e.g. Binary, Struct, Dict, temporal, rare types)
// ---------------------------------------------------------------------------

macro_rules! write_int {
  ($buf:expr, $arr:expr, $array_type:ty, $row:expr) => {{
    let a = $arr.as_any().downcast_ref::<$array_type>().unwrap();
    let mut b = itoa::Buffer::new();
    $buf.push_str(b.format(a.value($row)));
  }};
}

fn write_value(arr: &dyn Array, row: usize, buf: &mut String) {
  if arr.is_null(row) {
    buf.push_str("null");
    return;
  }
  match arr.data_type() {
    DataType::Boolean => {
      let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
      buf.push_str(if a.value(row) { "true" } else { "false" });
    }
    DataType::Int8 => write_int!(buf, arr, arrow_array::Int8Array, row),
    DataType::Int16 => write_int!(buf, arr, arrow_array::Int16Array, row),
    DataType::Int32 => write_int!(buf, arr, arrow_array::Int32Array, row),
    DataType::Int64 => {
      let a = arr
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
      write_i64_val(a.value(row), buf);
    }
    DataType::UInt8 => write_int!(buf, arr, arrow_array::UInt8Array, row),
    DataType::UInt16 => write_int!(buf, arr, arrow_array::UInt16Array, row),
    DataType::UInt32 => write_int!(buf, arr, arrow_array::UInt32Array, row),
    DataType::UInt64 => {
      let a = arr
        .as_any()
        .downcast_ref::<arrow_array::UInt64Array>()
        .unwrap();
      write_u64_val(a.value(row), buf);
    }
    DataType::Float16 => {
      let a = arr
        .as_any()
        .downcast_ref::<arrow_array::Float16Array>()
        .unwrap();
      write_f64_val(a.value(row).to_f64(), buf);
    }
    DataType::Float32 => {
      let a = arr
        .as_any()
        .downcast_ref::<arrow_array::Float32Array>()
        .unwrap();
      write_f64_val(a.value(row) as f64, buf);
    }
    DataType::Float64 => {
      let a = arr
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .unwrap();
      write_f64_val(a.value(row), buf);
    }
    DataType::Utf8 => {
      let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
      write_json_str(buf, a.value(row));
    }
    DataType::LargeUtf8 => {
      let a = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
      write_json_str(buf, a.value(row));
    }
    DataType::Binary => {
      let a = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
      write_byte_array(buf, a.value(row));
    }
    DataType::LargeBinary => {
      let a = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
      write_byte_array(buf, a.value(row));
    }
    DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
      write_list_value(arr, row, buf);
    }
    DataType::Struct(fields) => write_struct_value(arr, fields, row, buf),
    DataType::Map(entry_field, _sorted) => write_map_value(arr, entry_field, row, buf),
    DataType::Dictionary(_, _) => write_dict_value(arr, row, buf),
    DataType::Timestamp(_, _)
    | DataType::Date32
    | DataType::Date64
    | DataType::Time32(_)
    | DataType::Time64(_)
    | DataType::Duration(_)
    | DataType::Interval(_) => match arrow_cast::cast(arr, &DataType::Utf8) {
      Ok(casted) => {
        let sa = casted.as_any().downcast_ref::<StringArray>().unwrap();
        if sa.is_null(row) {
          buf.push_str("null");
        } else {
          write_json_str(buf, sa.value(row));
        }
      }
      Err(_) => {
        buf.push('"');
        let _ = write!(buf, "<unsupported:{:?}>", arr.data_type());
        buf.push('"');
      }
    },
    _ => {
      buf.push('"');
      let _ = write!(buf, "<unsupported:{:?}>", arr.data_type());
      buf.push('"');
    }
  }
}

fn write_byte_array(buf: &mut String, bytes: &[u8]) {
  buf.push('[');
  for (i, &b) in bytes.iter().enumerate() {
    if i > 0 {
      buf.push(',');
    }
    let mut ib = itoa::Buffer::new();
    buf.push_str(ib.format(b));
  }
  buf.push(']');
}

fn write_list_value(arr: &dyn Array, row: usize, buf: &mut String) {
  if let Some(a) = arr.as_any().downcast_ref::<ListArray>() {
    let values = a.values();
    let start = a.value_offsets()[row] as usize;
    let end = a.value_offsets()[row + 1] as usize;
    buf.push('[');
    for i in start..end {
      if i > start {
        buf.push(',');
      }
      write_value(values.as_ref(), i, buf);
    }
    buf.push(']');
  } else if let Some(a) = arr.as_any().downcast_ref::<LargeListArray>() {
    let values = a.values();
    let start = a.value_offsets()[row] as usize;
    let end = a.value_offsets()[row + 1] as usize;
    buf.push('[');
    for i in start..end {
      if i > start {
        buf.push(',');
      }
      write_value(values.as_ref(), i, buf);
    }
    buf.push(']');
  } else if let Some(a) = arr.as_any().downcast_ref::<FixedSizeListArray>() {
    let values = a.values();
    let size = a.value_length() as usize;
    let start = row * size;
    let end = start + size;
    buf.push('[');
    for i in start..end {
      if i > start {
        buf.push(',');
      }
      write_value(values.as_ref(), i, buf);
    }
    buf.push(']');
  } else {
    buf.push_str("null");
  }
}

fn write_struct_value(
  arr: &dyn Array,
  fields: &arrow_schema::Fields,
  row: usize,
  buf: &mut String,
) {
  let sa = arr.as_any().downcast_ref::<StructArray>().unwrap();
  buf.push('{');
  let mut first = true;
  for (i, field) in fields.iter().enumerate() {
    let col = sa.column(i).as_ref();
    if !col.is_null(row) {
      if !first {
        buf.push(',');
      }
      first = false;
      write_json_str(buf, field.name());
      buf.push(':');
      write_value(col, row, buf);
    }
  }
  buf.push('}');
}

fn write_map_value(arr: &dyn Array, entry_field: &Field, row: usize, buf: &mut String) {
  let ma = arr.as_any().downcast_ref::<MapArray>().unwrap();
  let start = ma.value_offsets()[row] as usize;
  let end = ma.value_offsets()[row + 1] as usize;
  if start == end {
    buf.push_str("null");
    return;
  }

  let entries = ma.entries();
  let struct_fields = match entry_field.data_type() {
    DataType::Struct(f) => f,
    _ => {
      buf.push_str("null");
      return;
    }
  };

  let key_col = entries.column(0);
  let val_col = entries.column(1);
  let key_is_utf8 = matches!(
    struct_fields[0].data_type(),
    DataType::Utf8 | DataType::LargeUtf8
  );

  if key_is_utf8 {
    let key_utf8 = key_col.as_any().downcast_ref::<StringArray>();
    let key_large = key_col.as_any().downcast_ref::<LargeStringArray>();
    buf.push('{');
    for i in start..end {
      if i > start {
        buf.push(',');
      }
      let key = if let Some(a) = key_utf8 {
        a.value(i)
      } else if let Some(a) = key_large {
        a.value(i)
      } else {
        continue;
      };
      write_json_str(buf, key);
      buf.push(':');
      write_value(val_col.as_ref(), i, buf);
    }
    buf.push('}');
  } else {
    buf.push('[');
    for i in start..end {
      if i > start {
        buf.push(',');
      }
      buf.push_str("{\"key\":");
      write_value(key_col.as_ref(), i, buf);
      buf.push_str(",\"value\":");
      write_value(val_col.as_ref(), i, buf);
      buf.push('}');
    }
    buf.push(']');
  }
}

fn write_dict_value(arr: &dyn Array, row: usize, buf: &mut String) {
  macro_rules! try_dict {
    ($arr:expr, $key_ty:ty, $row:expr, $buf:expr) => {
      if let Some(da) = $arr.as_any().downcast_ref::<DictionaryArray<$key_ty>>() {
        let key = da.keys().value($row) as usize;
        write_value(da.values().as_ref(), key, $buf);
        return;
      }
    };
  }

  try_dict!(arr, Int8Type, row, buf);
  try_dict!(arr, Int16Type, row, buf);
  try_dict!(arr, Int32Type, row, buf);
  try_dict!(arr, Int64Type, row, buf);
  try_dict!(arr, UInt8Type, row, buf);
  try_dict!(arr, UInt16Type, row, buf);
  try_dict!(arr, UInt32Type, row, buf);
  try_dict!(arr, UInt64Type, row, buf);

  buf.push_str("null");
}

// ---------------------------------------------------------------------------
// Batch → JSON direct writer (hot loop)
// ---------------------------------------------------------------------------

struct ColMeta<'a> {
  json_key: String,
  writer: ColWriter<'a>,
  check_skip: bool,
  has_nulls: bool,
  col: &'a dyn Array,
}

fn write_batches_json(batches: &[RecordBatch], buf: &mut String) {
  buf.push('[');
  let mut first_row = true;

  for batch in batches {
    let schema = batch.schema();
    let num_rows = batch.num_rows();

    let cols: Vec<ColMeta> = schema
      .fields()
      .iter()
      .enumerate()
      .map(|(i, f)| {
        let col = batch.column(i).as_ref();
        let mut json_key = String::with_capacity(f.name().len() + 4);
        write_json_str(&mut json_key, f.name());
        json_key.push(':');
        let has_nulls = col.null_count() > 0;
        let is_map = matches!(f.data_type(), DataType::Map(..));
        ColMeta {
          json_key,
          writer: resolve_writer(col),
          check_skip: has_nulls || is_map,
          has_nulls,
          col,
        }
      })
      .collect();

    for row in 0..num_rows {
      if !first_row {
        buf.push(',');
      }
      first_row = false;
      buf.push('{');
      let mut first_field = true;
      for cm in &cols {
        if cm.check_skip {
          if cm.has_nulls && cm.col.is_null(row) {
            continue;
          }
          if should_skip_writer(&cm.writer, row) {
            continue;
          }
        }
        if !first_field {
          buf.push(',');
        }
        first_field = false;
        buf.push_str(&cm.json_key);
        write_col(&cm.writer, row, buf);
      }
      buf.push('}');
    }
  }

  buf.push(']');
}

// ---------------------------------------------------------------------------
// IPC parsing
// ---------------------------------------------------------------------------

const ARROW_FILE_MAGIC: &[u8] = b"ARROW1";

fn is_ipc_file(data: &[u8]) -> bool {
  data.len() >= 8 && data[..6] == *ARROW_FILE_MAGIC
}

fn read_batches(bytes: &[u8]) -> napi::Result<Vec<RecordBatch>> {
  let mut batches = Vec::new();
  if is_ipc_file(bytes) {
    let cursor = std::io::Cursor::new(bytes);
    let reader = FileReader::try_new(cursor, None)
      .map_err(|e| napi::Error::from_reason(format!("Failed to read Arrow IPC file: {e}")))?;
    for batch_result in reader {
      batches.push(
        batch_result.map_err(|e| napi::Error::from_reason(format!("Failed to read batch: {e}")))?,
      );
    }
  } else {
    let cursor = std::io::Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None)
      .map_err(|e| napi::Error::from_reason(format!("Failed to read Arrow IPC stream: {e}")))?;
    for batch_result in reader {
      batches.push(
        batch_result.map_err(|e| napi::Error::from_reason(format!("Failed to read batch: {e}")))?,
      );
    }
  }
  Ok(batches)
}

// ---------------------------------------------------------------------------
// N-API exports
// ---------------------------------------------------------------------------

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
  let batches = read_batches(bytes)?;

  let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
  let mut buf = String::with_capacity(total_rows * 128);
  write_batches_json(&batches, &mut buf);
  Ok(buf)
}

#[napi(object)]
pub struct TimedResult {
  pub json: String,
  pub ipc_parse_us: f64,
  pub json_write_us: f64,
  pub total_us: f64,
  pub rows: u32,
  pub json_bytes: u32,
}

#[napi]
pub fn arrow_ipc_to_json_timed(data: Buffer) -> napi::Result<TimedResult> {
  let t_total = std::time::Instant::now();
  let bytes = data.as_ref();

  let t_ipc = std::time::Instant::now();
  let batches = read_batches(bytes)?;
  let ipc_parse_us = t_ipc.elapsed().as_micros() as f64;

  let t_write = std::time::Instant::now();
  let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
  let mut buf = String::with_capacity(total_rows * 128);
  write_batches_json(&batches, &mut buf);
  let json_write_us = t_write.elapsed().as_micros() as f64;

  let json_bytes = buf.len() as u32;
  let total_us = t_total.elapsed().as_micros() as f64;

  Ok(TimedResult {
    json: buf,
    ipc_parse_us,
    json_write_us,
    total_us,
    rows: total_rows as u32,
    json_bytes,
  })
}
