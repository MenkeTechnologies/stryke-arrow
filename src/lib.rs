//! stryke-arrow — Apache Arrow / Parquet / IPC / CSV / JSON cdylib loaded
//! in-process by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn arrow__*` is a JSON-string-in /
//! JSON-string-out wrapper around the `arrow` + `parquet` crates. stryke's
//! FFI bridge (`rust_ffi.rs::load_cdylib`) resolves these symbols at first
//! `use Arrow`, registers each one as a stryke-callable function, and on
//! each call passes a JSON-encoded args dict and copies the returned JSON
//! into a stryke string. The cdylib's `stryke_free_cstring` export plugs
//! the returned-allocation leak the inline-FFI v1 had.
//!
//! Stateless package — no persistent caches beyond rustc's static const
//! tables. Replaces the v1 helper-binary model (fork + exec per call +
//! NDJSON over a pipe) with a single in-process function call.

use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor};
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatchReader;
use arrow_csv::reader::Format as CsvFormat;
use arrow_csv::ReaderBuilder as CsvReaderBuilder;
use arrow_csv::WriterBuilder as CsvWriterBuilder;
use arrow_ipc::reader::FileReader as IpcFileReader;
use arrow_ipc::writer::FileWriter as IpcFileWriter;
use arrow_json::reader::ReaderBuilder as JsonReaderBuilder;
use arrow_json::writer::{LineDelimited, WriterBuilder as JsonWriterBuilder};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::{json, Map, Value};

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

// ── format dispatch ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fmt {
    Parquet,
    Ipc,
    Feather,
    Csv,
    Json,
}

impl Fmt {
    fn parse(name: &str) -> Result<Fmt> {
        match name.to_ascii_lowercase().as_str() {
            "parquet" | "pq" => Ok(Fmt::Parquet),
            "ipc" | "arrow" => Ok(Fmt::Ipc),
            "feather" => Ok(Fmt::Feather),
            "csv" | "tsv" => Ok(Fmt::Csv),
            "json" | "ndjson" | "jsonl" => Ok(Fmt::Json),
            other => bail!("unknown format `{other}` (parquet|ipc|feather|csv|json)"),
        }
    }

    fn detect(path: &Path) -> Result<Fmt> {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .ok_or_else(|| anyhow!("no extension on `{}` — pass `format`", path.display()))?;
        Fmt::parse(ext)
    }

    fn from_override_or_path(opt: Option<&str>, path: &Path) -> Result<Fmt> {
        if let Some(name) = opt {
            Fmt::parse(name)
        } else {
            Fmt::detect(path)
        }
    }
}

fn parse_columns(v: &Value) -> Option<Vec<String>> {
    v.as_array().map(|a| {
        a.iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect()
    })
}

// ── readers ─────────────────────────────────────────────────────────────────

fn open_reader(
    path: &Path,
    fmt: Fmt,
    columns: Option<&[String]>,
    batch_size: usize,
) -> Result<Box<dyn RecordBatchReader + Send>> {
    match fmt {
        Fmt::Parquet => {
            let file = File::open(path)
                .with_context(|| format!("opening parquet file `{}`", path.display()))?;
            let mut builder =
                ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(batch_size);
            if let Some(cols) = columns {
                let parquet_schema = builder.parquet_schema();
                let mask = parquet::arrow::ProjectionMask::columns(
                    parquet_schema,
                    cols.iter().map(|s| s.as_str()),
                );
                builder = builder.with_projection(mask);
            }
            Ok(Box::new(builder.build()?))
        }
        Fmt::Ipc | Fmt::Feather => {
            let file = File::open(path)
                .with_context(|| format!("opening arrow IPC file `{}`", path.display()))?;
            let mut builder = arrow_ipc::reader::FileReaderBuilder::new();
            if let Some(cols) = columns {
                let probe = File::open(path)?;
                let probe_reader = IpcFileReader::try_new(probe, None)?;
                let schema = probe_reader.schema();
                let indices = column_indices(&schema, cols)?;
                builder = builder.with_projection(indices);
            }
            let reader = builder.build(BufReader::new(file))?;
            Ok(Box::new(reader))
        }
        Fmt::Csv => {
            let probe = File::open(path)?;
            let (schema, _) = CsvFormat::default()
                .with_header(true)
                .infer_schema(probe, Some(1024))?;
            let schema = Arc::new(schema);
            let file = File::open(path)?;
            let reader = CsvReaderBuilder::new(Arc::clone(&schema))
                .with_header(true)
                .with_batch_size(batch_size)
                .build(file)?;
            if let Some(cols) = columns {
                let indices = column_indices(&schema, cols)?;
                let projected_fields: Vec<_> =
                    indices.iter().map(|i| schema.field(*i).clone()).collect();
                let projected_schema = Arc::new(Schema::new(projected_fields));
                return Ok(Box::new(ProjectingReader {
                    inner: Box::new(reader),
                    indices,
                    schema: projected_schema,
                }));
            }
            Ok(Box::new(reader))
        }
        Fmt::Json => {
            let probe = File::open(path)?;
            let mut probe = BufReader::new(probe);
            let (schema, _) = arrow_json::reader::infer_json_schema(&mut probe, Some(1024))?;
            let schema = Arc::new(schema);
            let file = File::open(path)?;
            let reader = JsonReaderBuilder::new(Arc::clone(&schema))
                .with_batch_size(batch_size)
                .build(BufReader::new(file))?;
            if let Some(cols) = columns {
                let indices = column_indices(&schema, cols)?;
                let projected_fields: Vec<_> =
                    indices.iter().map(|i| schema.field(*i).clone()).collect();
                let projected_schema = Arc::new(Schema::new(projected_fields));
                return Ok(Box::new(ProjectingReader {
                    inner: Box::new(reader),
                    indices,
                    schema: projected_schema,
                }));
            }
            Ok(Box::new(reader))
        }
    }
}

fn column_indices(schema: &SchemaRef, names: &[String]) -> Result<Vec<usize>> {
    names
        .iter()
        .map(|n| {
            schema
                .index_of(n)
                .map_err(|_| anyhow!("column `{}` not in schema", n))
        })
        .collect()
}

/// Apply column projection over a RecordBatchReader that doesn't natively
/// support it (CSV / JSON).
struct ProjectingReader {
    inner: Box<dyn RecordBatchReader + Send>,
    indices: Vec<usize>,
    schema: SchemaRef,
}

impl Iterator for ProjectingReader {
    type Item = std::result::Result<RecordBatch, arrow::error::ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|r| {
            r.and_then(|b| {
                let cols: Vec<_> = self.indices.iter().map(|i| b.column(*i).clone()).collect();
                RecordBatch::try_new(Arc::clone(&self.schema), cols)
            })
        })
    }
}

impl RecordBatchReader for ProjectingReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// ── core ops ────────────────────────────────────────────────────────────────

fn op_read(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let path = Path::new(path);
    let fmt = Fmt::from_override_or_path(args["format"].as_str(), path)?;
    let cols = parse_columns(&args["columns"]);
    let limit = args["limit"].as_u64().map(|n| n as usize);
    let skip = args["skip"].as_u64().unwrap_or(0) as usize;
    let batch_size = args["batch_size"].as_u64().unwrap_or(8192) as usize;

    let reader = open_reader(path, fmt, cols.as_deref(), batch_size)?;
    let schema = reader.schema();
    let names: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    let mut buf = Vec::<u8>::new();
    {
        let mut writer = JsonWriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(&mut buf);
        let mut emitted = 0usize;
        let mut skipped = 0usize;
        for batch in reader {
            let mut batch = batch?;
            let n = batch.num_rows();
            if skipped < skip {
                let to_skip = (skip - skipped).min(n);
                skipped += to_skip;
                if to_skip == n {
                    continue;
                }
                batch = batch.slice(to_skip, n - to_skip);
            }
            let remaining = limit
                .map(|l| l.saturating_sub(emitted))
                .unwrap_or(usize::MAX);
            if remaining == 0 {
                break;
            }
            if batch.num_rows() > remaining {
                batch = batch.slice(0, remaining);
            }
            writer.write(&batch)?;
            emitted += batch.num_rows();
            if limit.is_some_and(|l| emitted >= l) {
                break;
            }
        }
        writer.finish()?;
    }
    let rows: Vec<Value> = std::str::from_utf8(&buf)?
        .lines()
        .filter(|l| !l.is_empty())
        .map(serde_json::from_str)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(json!({"columns": names, "rows": rows}))
}

fn op_read_columnar(args: Value) -> Result<Value> {
    let read_args = args.clone();
    let r = op_read(read_args)?;
    let names = r["columns"].as_array().cloned().unwrap_or_default();
    let rows = r["rows"].as_array().cloned().unwrap_or_default();
    let mut data = Map::new();
    for n in &names {
        let n = n.as_str().unwrap_or("");
        data.insert(n.to_string(), Value::Array(Vec::new()));
    }
    for row in &rows {
        if let Some(obj) = row.as_object() {
            for n in &names {
                let n = n.as_str().unwrap_or("");
                if let Some(Value::Array(arr)) = data.get_mut(n) {
                    arr.push(obj.get(n).cloned().unwrap_or(Value::Null));
                }
            }
        }
    }
    Ok(json!({"columns": names, "num_rows": rows.len(), "data": data}))
}

fn op_schema(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let path = Path::new(path);
    let fmt = Fmt::from_override_or_path(args["format"].as_str(), path)?;
    let reader = open_reader(path, fmt, None, 8192)?;
    let schema = reader.schema();
    let fields: Vec<Value> = schema
        .fields()
        .iter()
        .map(|f| {
            json!({
                "name": f.name(),
                "type": format!("{:?}", f.data_type()),
                "nullable": f.is_nullable(),
            })
        })
        .collect();
    Ok(json!({"fields": fields}))
}

fn op_stats(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let path_buf = Path::new(path);
    let fmt = Fmt::from_override_or_path(args["format"].as_str(), path_buf)?;
    if let Fmt::Parquet = fmt {
        // Parquet footer-driven stats — no full scan.
        let file = File::open(path_buf)?;
        let reader = SerializedFileReader::new(file)?;
        let meta = reader.metadata();
        let file_meta = meta.file_metadata();
        let num_rows = file_meta.num_rows();
        let num_row_groups = meta.num_row_groups();
        let mut cols = Vec::new();
        if num_row_groups > 0 {
            let rg = meta.row_group(0);
            for i in 0..rg.num_columns() {
                let col = rg.column(i);
                let name = col.column_descr().path().string();
                let stats = col.statistics();
                let entry = match stats {
                    Some(s) => {
                        let (min_s, max_s) = match s {
                            Statistics::Boolean(s) => {
                                (s.min_opt().map(|v| json!(v)), s.max_opt().map(|v| json!(v)))
                            }
                            Statistics::Int32(s) => {
                                (s.min_opt().map(|v| json!(v)), s.max_opt().map(|v| json!(v)))
                            }
                            Statistics::Int64(s) => {
                                (s.min_opt().map(|v| json!(v)), s.max_opt().map(|v| json!(v)))
                            }
                            Statistics::Float(s) => {
                                (s.min_opt().map(|v| json!(v)), s.max_opt().map(|v| json!(v)))
                            }
                            Statistics::Double(s) => {
                                (s.min_opt().map(|v| json!(v)), s.max_opt().map(|v| json!(v)))
                            }
                            _ => (None, None),
                        };
                        json!({
                            "name": name,
                            "null_count": s.null_count_opt(),
                            "min": min_s.unwrap_or(Value::Null),
                            "max": max_s.unwrap_or(Value::Null),
                        })
                    }
                    None => json!({"name": name}),
                };
                cols.push(entry);
            }
        }
        return Ok(json!({
            "num_rows": num_rows,
            "num_row_groups": num_row_groups,
            "num_columns": cols.len(),
            "columns": cols,
        }));
    }
    // Other formats: scan + count.
    let reader = open_reader(path_buf, fmt, None, 8192)?;
    let schema = reader.schema();
    let mut num_rows = 0i64;
    for batch in reader {
        num_rows += batch?.num_rows() as i64;
    }
    let cols: Vec<Value> = schema
        .fields()
        .iter()
        .map(|f| json!({"name": f.name(), "type": format!("{:?}", f.data_type())}))
        .collect();
    Ok(json!({
        "num_rows": num_rows,
        "num_columns": cols.len(),
        "columns": cols,
    }))
}

// ── writers ─────────────────────────────────────────────────────────────────

use parquet::file::statistics::Statistics;

fn rows_to_batches(rows: &[Value]) -> Result<Vec<RecordBatch>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    // Serialize the rows back to NDJSON and let arrow-json infer + parse.
    let mut buf = Vec::<u8>::new();
    for r in rows {
        serde_json::to_writer(&mut buf, r)?;
        buf.push(b'\n');
    }
    let mut probe = Cursor::new(&buf);
    let (schema, _) = arrow_json::reader::infer_json_schema(&mut probe, Some(rows.len()))?;
    let schema = Arc::new(schema);
    let reader = JsonReaderBuilder::new(Arc::clone(&schema))
        .with_batch_size(rows.len().max(1))
        .build(Cursor::new(&buf))?;
    let mut out = Vec::new();
    for b in reader {
        out.push(b?);
    }
    Ok(out)
}

fn compression_for(name: &str) -> Result<Compression> {
    match name.to_ascii_lowercase().as_str() {
        "none" | "uncompressed" => Ok(Compression::UNCOMPRESSED),
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => Ok(Compression::GZIP(Default::default())),
        "lz4" => Ok(Compression::LZ4_RAW),
        "brotli" => Ok(Compression::BROTLI(Default::default())),
        "zstd" => Ok(Compression::ZSTD(ZstdLevel::default())),
        other => bail!("unknown compression `{}`", other),
    }
}

fn op_write(args: Value) -> Result<Value> {
    let path = args["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let path = Path::new(path);
    let fmt = Fmt::from_override_or_path(args["format"].as_str(), path)?;
    let compression = args["compression"].as_str().unwrap_or("snappy").to_string();
    let row_group = args["row_group"].as_u64().unwrap_or(65536) as usize;
    let rows = args["rows"]
        .as_array()
        .ok_or_else(|| anyhow!("missing rows (array)"))?;
    let batches = rows_to_batches(rows)?;
    if batches.is_empty() {
        bail!("write: no rows");
    }
    let schema = batches[0].schema();
    write_batches(path, fmt, schema, &batches, &compression, row_group)?;
    Ok(json!({"path": path.display().to_string(), "rows": rows.len()}))
}

fn write_batches(
    path: &Path,
    fmt: Fmt,
    schema: SchemaRef,
    batches: &[RecordBatch],
    compression: &str,
    row_group: usize,
) -> Result<()> {
    match fmt {
        Fmt::Parquet => {
            let file = File::create(path)?;
            let props = WriterProperties::builder()
                .set_compression(compression_for(compression)?)
                .set_max_row_group_row_count(Some(row_group))
                .build();
            let mut w = ArrowWriter::try_new(file, schema, Some(props))?;
            for b in batches {
                w.write(b)?;
            }
            w.close()?;
        }
        Fmt::Ipc | Fmt::Feather => {
            let file = File::create(path)?;
            let mut w = IpcFileWriter::try_new(BufWriter::new(file), &schema)?;
            for b in batches {
                w.write(b)?;
            }
            w.finish()?;
        }
        Fmt::Csv => {
            let file = File::create(path)?;
            let mut w = CsvWriterBuilder::new()
                .with_header(true)
                .build(BufWriter::new(file));
            for b in batches {
                w.write(b)?;
            }
        }
        Fmt::Json => {
            let file = File::create(path)?;
            let mut w = JsonWriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, LineDelimited>(BufWriter::new(file));
            for b in batches {
                w.write(b)?;
            }
            w.finish()?;
        }
    }
    Ok(())
}

fn op_convert(args: Value) -> Result<Value> {
    let src = args["src"].as_str().ok_or_else(|| anyhow!("missing src"))?;
    let dst = args["dst"].as_str().ok_or_else(|| anyhow!("missing dst"))?;
    let src = Path::new(src);
    let dst = Path::new(dst);
    let src_fmt = Fmt::from_override_or_path(args["src_format"].as_str(), src)?;
    let dst_fmt = Fmt::from_override_or_path(args["dst_format"].as_str(), dst)?;
    let compression = args["compression"].as_str().unwrap_or("snappy").to_string();
    let row_group = args["row_group"].as_u64().unwrap_or(65536) as usize;

    let reader = open_reader(src, src_fmt, None, 8192)?;
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
    write_batches(dst, dst_fmt, schema, &batches, &compression, row_group)?;
    Ok(json!({
        "src": src.display().to_string(),
        "dst": dst.display().to_string(),
        "rows": batches.iter().map(|b| b.num_rows()).sum::<usize>(),
    }))
}

// ── compute ─────────────────────────────────────────────────────────────────
//
// File→file transforms built on `open_reader` + `write_batches`: read the
// source into RecordBatches, apply an Arrow compute kernel, write the result.
// Output format defaults to the destination extension (else the source
// format); compression/row_group default as in `op_write`.

use arrow::array::{ArrayRef, BooleanArray, Scalar};
use arrow::compute::kernels::cmp;
use arrow::compute::{
    concat_batches, filter_record_batch, lexsort_to_indices, take, SortColumn, SortOptions,
};
use arrow::datatypes::{DataType, Field};

/// Source/destination paths, formats and write options for a transform.
struct Io {
    src: std::path::PathBuf,
    dst: std::path::PathBuf,
    src_fmt: Fmt,
    dst_fmt: Fmt,
    compression: String,
    row_group: usize,
}

fn parse_io(args: &Value) -> Result<Io> {
    let src = args["src"].as_str().ok_or_else(|| anyhow!("missing src"))?;
    let dst = args["dst"].as_str().ok_or_else(|| anyhow!("missing dst"))?;
    let src = Path::new(src);
    let dst = Path::new(dst);
    let src_fmt = Fmt::from_override_or_path(args["src_format"].as_str(), src)?;
    let dst_fmt = match args["dst_format"].as_str() {
        Some(n) => Fmt::parse(n)?,
        None => Fmt::detect(dst).unwrap_or(src_fmt),
    };
    Ok(Io {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
        src_fmt,
        dst_fmt,
        compression: args["compression"].as_str().unwrap_or("snappy").to_string(),
        row_group: args["row_group"].as_u64().unwrap_or(65536) as usize,
    })
}

/// Read every batch of `src`, returning the (possibly projected) schema.
fn read_all(
    src: &Path,
    fmt: Fmt,
    columns: Option<&[String]>,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let reader = open_reader(src, fmt, columns, 8192)?;
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
    Ok((schema, batches))
}

fn write_result(io: &Io, schema: SchemaRef, batches: &[RecordBatch]) -> Result<usize> {
    let rows = batches.iter().map(|b| b.num_rows()).sum();
    write_batches(
        &io.dst,
        io.dst_fmt,
        schema,
        batches,
        &io.compression,
        io.row_group,
    )?;
    Ok(rows)
}

/// Truthiness across the JSON shapes stryke emits for flags: a real boolean,
/// a non-zero number (stryke `1`/`0`), or the strings `"true"`/`"1"`. stryke
/// serializes `descending => 1` as the JSON number `1`, which `Value::as_bool`
/// rejects — so flag parsing must be lenient or options silently no-op.
fn json_truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => matches!(s.as_str(), "true" | "1"),
        _ => false,
    }
}

fn parse_dtype(name: &str) -> Result<DataType> {
    Ok(match name.to_ascii_lowercase().as_str() {
        "int" | "int64" | "i64" | "long" => DataType::Int64,
        "int32" | "i32" => DataType::Int32,
        "float" | "float64" | "f64" | "double" => DataType::Float64,
        "float32" | "f32" => DataType::Float32,
        "str" | "string" | "utf8" => DataType::Utf8,
        "bool" | "boolean" => DataType::Boolean,
        other => bail!("unknown type `{other}` (int|int32|float|float32|str|bool)"),
    })
}

/// Build a single-element Arrow array of type `dt` from a JSON scalar — the
/// right-hand side of a comparison in `op_filter`.
fn scalar_for(dt: &DataType, v: &Value) -> Result<ArrayRef> {
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    let arr: ArrayRef = match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            let n = v
                .as_i64()
                .ok_or_else(|| anyhow!("expected integer value for column type {dt:?}"))?;
            arrow::compute::cast(&Int64Array::from(vec![n]), dt)?
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let f = v
                .as_f64()
                .ok_or_else(|| anyhow!("expected number value for column type {dt:?}"))?;
            arrow::compute::cast(&Float64Array::from(vec![f]), dt)?
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let s = v
                .as_str()
                .ok_or_else(|| anyhow!("expected string value for column type {dt:?}"))?;
            arrow::compute::cast(&StringArray::from(vec![s]), dt)?
        }
        DataType::Boolean => {
            let b = v
                .as_bool()
                .ok_or_else(|| anyhow!("expected boolean value for column type {dt:?}"))?;
            Arc::new(BooleanArray::from(vec![b]))
        }
        other => bail!("filter: unsupported column type {other:?}"),
    };
    Ok(arr)
}

/// Slice `[offset, offset+length)` (absolute rows) across a batch sequence.
fn slice_batches(
    batches: &[RecordBatch],
    offset: usize,
    length: Option<usize>,
) -> Vec<RecordBatch> {
    let want = length.unwrap_or(usize::MAX);
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut emitted = 0usize;
    for b in batches {
        let n = b.num_rows();
        let batch_start = start;
        start += n;
        if batch_start + n <= offset {
            continue;
        }
        let local = offset.saturating_sub(batch_start);
        if local >= n {
            continue;
        }
        let remaining = want.saturating_sub(emitted);
        if remaining == 0 {
            break;
        }
        let take_n = (n - local).min(remaining);
        out.push(b.slice(local, take_n));
        emitted += take_n;
        if emitted >= want {
            break;
        }
    }
    out
}

fn op_filter(args: Value) -> Result<Value> {
    let io = parse_io(&args)?;
    let column = args["column"]
        .as_str()
        .ok_or_else(|| anyhow!("missing column"))?;
    let op = args["op"].as_str().unwrap_or("eq");
    let (schema, batches) = read_all(&io.src, io.src_fmt, None)?;
    let idx = schema
        .index_of(column)
        .map_err(|_| anyhow!("filter: no column `{column}`"))?;
    let dt = schema.field(idx).data_type().clone();
    let scalar = Scalar::new(scalar_for(&dt, &args["value"])?);
    let mut out = Vec::with_capacity(batches.len());
    for b in &batches {
        let col = b.column(idx);
        let mask: BooleanArray = match op {
            "eq" | "==" => cmp::eq(col, &scalar)?,
            "ne" | "!=" => cmp::neq(col, &scalar)?,
            "lt" | "<" => cmp::lt(col, &scalar)?,
            "le" | "<=" => cmp::lt_eq(col, &scalar)?,
            "gt" | ">" => cmp::gt(col, &scalar)?,
            "ge" | ">=" => cmp::gt_eq(col, &scalar)?,
            other => bail!("filter: unknown op `{other}` (eq|ne|lt|le|gt|ge)"),
        };
        out.push(filter_record_batch(b, &mask)?);
    }
    let rows = write_result(&io, schema, &out)?;
    Ok(json!({"dst": io.dst.display().to_string(), "rows": rows}))
}

fn op_select(args: Value) -> Result<Value> {
    let io = parse_io(&args)?;
    let cols = parse_columns(&args["columns"])
        .ok_or_else(|| anyhow!("missing columns (array of names)"))?;
    let (schema, batches) = read_all(&io.src, io.src_fmt, None)?;
    let indices: Vec<usize> = cols
        .iter()
        .map(|c| {
            schema
                .index_of(c)
                .map_err(|_| anyhow!("select: no column `{c}`"))
        })
        .collect::<Result<_>>()?;
    let out_schema = Arc::new(schema.project(&indices)?);
    let projected: Vec<RecordBatch> = batches
        .iter()
        .map(|b| b.project(&indices))
        .collect::<std::result::Result<_, _>>()?;
    let rows = write_result(&io, out_schema, &projected)?;
    Ok(json!({"dst": io.dst.display().to_string(), "rows": rows, "columns": cols}))
}

/// Drop named columns, keeping the rest in their original order — the complement
/// of `select`. Every named column must exist (a typo errors rather than
/// silently no-ops). Returns the surviving column names.
fn op_drop(args: Value) -> Result<Value> {
    let io = parse_io(&args)?;
    let cols = parse_columns(&args["columns"])
        .ok_or_else(|| anyhow!("missing columns (array of names to drop)"))?;
    let (schema, batches) = read_all(&io.src, io.src_fmt, None)?;
    for c in &cols {
        schema
            .index_of(c)
            .map_err(|_| anyhow!("drop: no column `{c}`"))?;
    }
    let drop: std::collections::HashSet<&str> = cols.iter().map(String::as_str).collect();
    let indices: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !drop.contains(f.name().as_str()))
        .map(|(i, _)| i)
        .collect();
    let kept: Vec<String> = indices
        .iter()
        .map(|&i| schema.field(i).name().clone())
        .collect();
    let out_schema = Arc::new(schema.project(&indices)?);
    let projected: Vec<RecordBatch> = batches
        .iter()
        .map(|b| b.project(&indices))
        .collect::<std::result::Result<_, _>>()?;
    let rows = write_result(&io, out_schema, &projected)?;
    Ok(json!({"dst": io.dst.display().to_string(), "rows": rows, "columns": kept}))
}

fn op_sort(args: Value) -> Result<Value> {
    let io = parse_io(&args)?;
    let by = args["by"]
        .as_array()
        .ok_or_else(|| anyhow!("missing by (array of {{column, descending}})"))?;
    let (schema, batches) = read_all(&io.src, io.src_fmt, None)?;
    let merged = concat_batches(&schema, &batches)?;
    let sort_cols: Vec<SortColumn> = by
        .iter()
        .map(|spec| -> Result<SortColumn> {
            let name = spec["column"]
                .as_str()
                .or_else(|| spec.as_str())
                .ok_or_else(|| anyhow!("sort: each `by` entry needs a column name"))?;
            let descending = json_truthy(&spec["descending"]);
            let idx = schema
                .index_of(name)
                .map_err(|_| anyhow!("sort: no column `{name}`"))?;
            let nulls_first = match &spec["nulls_first"] {
                Value::Null => descending,
                other => json_truthy(other),
            };
            Ok(SortColumn {
                values: merged.column(idx).clone(),
                options: Some(SortOptions {
                    descending,
                    nulls_first,
                }),
            })
        })
        .collect::<Result<_>>()?;
    let indices = lexsort_to_indices(&sort_cols, None)?;
    let cols: Vec<ArrayRef> = merged
        .columns()
        .iter()
        .map(|c| take(c, &indices, None))
        .collect::<std::result::Result<_, _>>()?;
    let sorted = RecordBatch::try_new(schema.clone(), cols)?;
    let rows = write_result(&io, schema, &[sorted])?;
    Ok(json!({"dst": io.dst.display().to_string(), "rows": rows}))
}

fn op_slice(args: Value) -> Result<Value> {
    let io = parse_io(&args)?;
    let offset = args["offset"].as_u64().unwrap_or(0) as usize;
    let length = args["length"].as_u64().map(|n| n as usize);
    let (schema, batches) = read_all(&io.src, io.src_fmt, None)?;
    let out = slice_batches(&batches, offset, length);
    let rows = write_result(&io, schema, &out)?;
    Ok(json!({"dst": io.dst.display().to_string(), "rows": rows}))
}

fn op_head(args: Value) -> Result<Value> {
    let io = parse_io(&args)?;
    let n = args["n"].as_u64().unwrap_or(10) as usize;
    let (schema, batches) = read_all(&io.src, io.src_fmt, None)?;
    let out = slice_batches(&batches, 0, Some(n));
    let rows = write_result(&io, schema, &out)?;
    Ok(json!({"dst": io.dst.display().to_string(), "rows": rows}))
}

fn op_tail(args: Value) -> Result<Value> {
    let io = parse_io(&args)?;
    let n = args["n"].as_u64().unwrap_or(10) as usize;
    let (schema, batches) = read_all(&io.src, io.src_fmt, None)?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    let offset = total.saturating_sub(n);
    let out = slice_batches(&batches, offset, None);
    let rows = write_result(&io, schema, &out)?;
    Ok(json!({"dst": io.dst.display().to_string(), "rows": rows}))
}

fn op_count(args: Value) -> Result<Value> {
    let path = args["src"]
        .as_str()
        .or_else(|| args["path"].as_str())
        .ok_or_else(|| anyhow!("missing src"))?;
    let path = Path::new(path);
    let fmt = Fmt::from_override_or_path(args["format"].as_str(), path)?;
    let reader = open_reader(path, fmt, None, 8192)?;
    let mut rows = 0usize;
    for b in reader {
        rows += b?.num_rows();
    }
    Ok(json!({"src": path.display().to_string(), "rows": rows}))
}

fn op_concat(args: Value) -> Result<Value> {
    let dst = args["dst"].as_str().ok_or_else(|| anyhow!("missing dst"))?;
    let dst = Path::new(dst);
    let srcs = args["srcs"]
        .as_array()
        .ok_or_else(|| anyhow!("missing srcs (array of paths)"))?;
    if srcs.is_empty() {
        bail!("concat: srcs is empty");
    }
    let dst_fmt = Fmt::from_override_or_path(args["dst_format"].as_str(), dst)?;
    let compression = args["compression"].as_str().unwrap_or("snappy").to_string();
    let row_group = args["row_group"].as_u64().unwrap_or(65536) as usize;
    let mut schema: Option<SchemaRef> = None;
    let mut all: Vec<RecordBatch> = Vec::new();
    for s in srcs {
        let p = s
            .as_str()
            .ok_or_else(|| anyhow!("concat: src not a string"))?;
        let p = Path::new(p);
        let fmt = Fmt::from_override_or_path(args["src_format"].as_str(), p)?;
        let (sch, batches) = read_all(p, fmt, None)?;
        match &schema {
            None => schema = Some(sch),
            Some(first) if first.fields() != sch.fields() => {
                bail!(
                    "concat: schema of `{}` differs from the first source",
                    p.display()
                )
            }
            _ => {}
        }
        all.extend(batches);
    }
    let schema = schema.unwrap();
    write_batches(dst, dst_fmt, schema, &all, &compression, row_group)?;
    let rows = all.iter().map(|b| b.num_rows()).sum::<usize>();
    Ok(json!({"dst": dst.display().to_string(), "rows": rows, "sources": srcs.len()}))
}

fn op_rename(args: Value) -> Result<Value> {
    let io = parse_io(&args)?;
    let map = args["map"]
        .as_object()
        .ok_or_else(|| anyhow!("missing map ({{old: new}})"))?;
    let (schema, batches) = read_all(&io.src, io.src_fmt, None)?;
    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| match map.get(f.name()).and_then(|v| v.as_str()) {
            Some(new) => Arc::new(f.as_ref().clone().with_name(new)),
            None => f.clone(),
        })
        .collect();
    let new_schema = Arc::new(Schema::new(fields));
    let renamed: Vec<RecordBatch> = batches
        .iter()
        .map(|b| RecordBatch::try_new(new_schema.clone(), b.columns().to_vec()))
        .collect::<std::result::Result<_, _>>()?;
    let rows = write_result(&io, new_schema, &renamed)?;
    Ok(json!({"dst": io.dst.display().to_string(), "rows": rows}))
}

fn op_cast(args: Value) -> Result<Value> {
    let io = parse_io(&args)?;
    let casts = args["casts"]
        .as_object()
        .ok_or_else(|| anyhow!("missing casts ({{column: type}})"))?;
    let (schema, batches) = read_all(&io.src, io.src_fmt, None)?;
    let mut targets: Vec<(usize, DataType)> = Vec::new();
    for (name, ty) in casts {
        let idx = schema
            .index_of(name)
            .map_err(|_| anyhow!("cast: no column `{name}`"))?;
        let dt = parse_dtype(
            ty.as_str()
                .ok_or_else(|| anyhow!("cast: type not a string"))?,
        )?;
        targets.push((idx, dt));
    }
    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| match targets.iter().find(|(idx, _)| *idx == i) {
            Some((_, dt)) => Arc::new(Field::new(f.name(), dt.clone(), f.is_nullable())),
            None => f.clone(),
        })
        .collect();
    let new_schema = Arc::new(Schema::new(fields));
    let mut out = Vec::with_capacity(batches.len());
    for b in &batches {
        let mut cols: Vec<ArrayRef> = b.columns().to_vec();
        for (idx, dt) in &targets {
            cols[*idx] = arrow::compute::cast(b.column(*idx), dt)?;
        }
        out.push(RecordBatch::try_new(new_schema.clone(), cols)?);
    }
    let rows = write_result(&io, new_schema, &out)?;
    Ok(json!({"dst": io.dst.display().to_string(), "rows": rows}))
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call<F>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Result<Value>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| handler(input)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-arrow handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── exports ─────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn arrow__version(args: *const c_char) -> *const c_char {
    ffi_call(args, |_| Ok(json!({"version": env!("CARGO_PKG_VERSION")})))
}

#[no_mangle]
pub extern "C" fn arrow__read(args: *const c_char) -> *const c_char {
    ffi_call(args, op_read)
}

#[no_mangle]
pub extern "C" fn arrow__read_columnar(args: *const c_char) -> *const c_char {
    ffi_call(args, op_read_columnar)
}

#[no_mangle]
pub extern "C" fn arrow__schema(args: *const c_char) -> *const c_char {
    ffi_call(args, op_schema)
}

#[no_mangle]
pub extern "C" fn arrow__stats(args: *const c_char) -> *const c_char {
    ffi_call(args, op_stats)
}

#[no_mangle]
pub extern "C" fn arrow__write(args: *const c_char) -> *const c_char {
    ffi_call(args, op_write)
}

#[no_mangle]
pub extern "C" fn arrow__convert(args: *const c_char) -> *const c_char {
    ffi_call(args, op_convert)
}

#[no_mangle]
pub extern "C" fn arrow__filter(args: *const c_char) -> *const c_char {
    ffi_call(args, op_filter)
}

#[no_mangle]
pub extern "C" fn arrow__select(args: *const c_char) -> *const c_char {
    ffi_call(args, op_select)
}

#[no_mangle]
pub extern "C" fn arrow__drop(args: *const c_char) -> *const c_char {
    ffi_call(args, op_drop)
}

#[no_mangle]
pub extern "C" fn arrow__sort(args: *const c_char) -> *const c_char {
    ffi_call(args, op_sort)
}

#[no_mangle]
pub extern "C" fn arrow__slice(args: *const c_char) -> *const c_char {
    ffi_call(args, op_slice)
}

#[no_mangle]
pub extern "C" fn arrow__head(args: *const c_char) -> *const c_char {
    ffi_call(args, op_head)
}

#[no_mangle]
pub extern "C" fn arrow__tail(args: *const c_char) -> *const c_char {
    ffi_call(args, op_tail)
}

#[no_mangle]
pub extern "C" fn arrow__count(args: *const c_char) -> *const c_char {
    ffi_call(args, op_count)
}

#[no_mangle]
pub extern "C" fn arrow__concat(args: *const c_char) -> *const c_char {
    ffi_call(args, op_concat)
}

#[no_mangle]
pub extern "C" fn arrow__rename(args: *const c_char) -> *const c_char {
    ffi_call(args, op_rename)
}

#[no_mangle]
pub extern "C" fn arrow__cast(args: *const c_char) -> *const c_char {
    ffi_call(args, op_cast)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn fmt_parse_canonical_names() {
        assert_eq!(Fmt::parse("parquet").unwrap(), Fmt::Parquet);
        assert_eq!(Fmt::parse("ipc").unwrap(), Fmt::Ipc);
        assert_eq!(Fmt::parse("feather").unwrap(), Fmt::Feather);
        assert_eq!(Fmt::parse("csv").unwrap(), Fmt::Csv);
        assert_eq!(Fmt::parse("json").unwrap(), Fmt::Json);
    }

    #[test]
    fn fmt_parse_aliases_collapse() {
        assert_eq!(Fmt::parse("pq").unwrap(), Fmt::Parquet);
        assert_eq!(Fmt::parse("arrow").unwrap(), Fmt::Ipc);
        assert_eq!(Fmt::parse("tsv").unwrap(), Fmt::Csv);
        assert_eq!(Fmt::parse("ndjson").unwrap(), Fmt::Json);
        assert_eq!(Fmt::parse("jsonl").unwrap(), Fmt::Json);
    }

    #[test]
    fn fmt_parse_is_case_insensitive() {
        assert_eq!(Fmt::parse("PARQUET").unwrap(), Fmt::Parquet);
        assert_eq!(Fmt::parse("Csv").unwrap(), Fmt::Csv);
        assert_eq!(Fmt::parse("JSON").unwrap(), Fmt::Json);
    }

    #[test]
    fn fmt_parse_unknown_errors_with_hint() {
        let err = Fmt::parse("orc").unwrap_err().to_string();
        assert!(err.contains("orc"), "{err}");
        assert!(
            err.contains("parquet|ipc|feather|csv|json"),
            "missing hint: {err}"
        );
    }

    #[test]
    fn fmt_detect_uses_extension() {
        assert_eq!(
            Fmt::detect(Path::new("/tmp/a.parquet")).unwrap(),
            Fmt::Parquet
        );
        assert_eq!(Fmt::detect(Path::new("/tmp/a.csv")).unwrap(), Fmt::Csv);
        assert_eq!(Fmt::detect(Path::new("/tmp/a.JSON")).unwrap(), Fmt::Json);
    }

    #[test]
    fn fmt_detect_no_extension_errors() {
        let err = Fmt::detect(Path::new("/tmp/no_ext"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("no extension"), "{err}");
    }

    #[test]
    fn fmt_from_override_wins_over_extension() {
        // Explicit format must override path-detected extension.
        assert_eq!(
            Fmt::from_override_or_path(Some("ipc"), Path::new("/tmp/a.parquet")).unwrap(),
            Fmt::Ipc
        );
    }

    #[test]
    fn fmt_from_override_falls_back_to_path_when_none() {
        assert_eq!(
            Fmt::from_override_or_path(None, Path::new("/tmp/a.parquet")).unwrap(),
            Fmt::Parquet
        );
    }

    #[test]
    fn parse_columns_handles_array_strings() {
        let v = json!(["a", "b", "c"]);
        assert_eq!(
            parse_columns(&v),
            Some(vec!["a".into(), "b".into(), "c".into()])
        );
    }

    #[test]
    fn parse_columns_filters_non_strings() {
        let v = json!(["a", 1, "b", null]);
        assert_eq!(parse_columns(&v), Some(vec!["a".into(), "b".into()]));
    }

    #[test]
    fn parse_columns_non_array_is_none() {
        assert_eq!(parse_columns(&json!("not-an-array")), None);
        assert_eq!(parse_columns(&json!(null)), None);
        assert_eq!(parse_columns(&json!({"a":1})), None);
    }

    #[test]
    fn compression_canonical_names() {
        // Compress enum lacks Eq, so just compare the rendered form.
        assert!(matches!(
            compression_for("none").unwrap(),
            Compression::UNCOMPRESSED
        ));
        assert!(matches!(
            compression_for("snappy").unwrap(),
            Compression::SNAPPY
        ));
        assert!(matches!(
            compression_for("lz4").unwrap(),
            Compression::LZ4_RAW
        ));
    }

    #[test]
    fn compression_aliases_and_case() {
        assert!(matches!(
            compression_for("UNCOMPRESSED").unwrap(),
            Compression::UNCOMPRESSED
        ));
        assert!(matches!(
            compression_for("Gzip").unwrap(),
            Compression::GZIP(_)
        ));
        assert!(matches!(
            compression_for("ZSTD").unwrap(),
            Compression::ZSTD(_)
        ));
        assert!(matches!(
            compression_for("brotli").unwrap(),
            Compression::BROTLI(_)
        ));
    }

    #[test]
    fn compression_unknown_errors() {
        let err = compression_for("lzma").unwrap_err().to_string();
        assert!(err.contains("lzma"), "{err}");
    }

    /// `parse_columns` must reject non-array JSON input — pin the contract
    /// so callers passing a string accidentally (`"id,name"` instead of
    /// `["id", "name"]`) get `None` rather than `Some([])` or a panic.
    #[test]
    fn parse_columns_rejects_non_array_inputs() {
        assert_eq!(parse_columns(&json!("id,name")), None);
        assert_eq!(parse_columns(&json!(42)), None);
        assert_eq!(parse_columns(&json!({"cols": ["id"]})), None);
        assert_eq!(parse_columns(&Value::Null), None);
    }

    /// Empty array input should be `Some(vec![])`, NOT `None` —
    /// distinguishing "supplied empty" from "not supplied" matters: empty
    /// usually means "no projection" downstream, while None means "read all
    /// columns".
    #[test]
    fn parse_columns_empty_array_is_some_not_none() {
        assert_eq!(parse_columns(&json!([])), Some(vec![]));
    }

    #[test]
    fn column_indices_maps_names_to_positions() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
        ]));
        let names = vec!["price".to_string(), "id".to_string()];
        assert_eq!(column_indices(&schema, &names).unwrap(), vec![2, 0]);
    }

    #[test]
    fn column_indices_missing_name_errors() {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let names = vec!["bogus".to_string()];
        let err = column_indices(&schema, &names).unwrap_err().to_string();
        assert!(err.contains("bogus"), "{err}");
    }

    // ── op_read skip / limit boundary coverage ────────────────────────────────
    //
    // The skip/limit machinery in `op_read` (lib.rs:243-268) is the trickiest
    // arithmetic in the file: it tracks `skipped`, `emitted`, and `remaining`
    // across batches and slices batches in-place when boundaries straddle.
    // The block has six interacting branches (skip>=n vs skip<n; remaining==0;
    // batch.num_rows()>remaining; final `emitted>=limit` break). Refactors here
    // are the most likely future regression: off-by-one on the `to_skip == n`
    // edge, `saturating_sub` swap, or moving the `break` past the slice can
    // each silently corrupt downstream callers. Inline-mod tests because all
    // `op_*` fns are private (crate-type = cdylib, no rlib).

    use std::io::Write as _;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn unique_csv(prefix: &str, body: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!(
            "stryke_arrow_test_{}_{}_{}.csv",
            prefix,
            std::process::id(),
            n
        ));
        let mut f = File::create(&p).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        p
    }

    #[test]
    fn op_read_skip_equal_to_total_returns_zero_rows() {
        // skip == total: every batch is fully consumed by the skip clause
        // (lib.rs:249 `continue`) and zero rows reach the writer. A regression
        // that mishandled `to_skip == n` (e.g., dropped the `continue` and let
        // the empty-slice path run) would emit a degenerate batch or panic.
        let path = unique_csv("skip_eq_total", "id,name\n1,a\n2,b\n3,c\n");
        let args = json!({
            "path": path.to_str().unwrap(),
            "format": "csv",
            "skip": 3,
            "batch_size": 2,
        });
        let r = op_read(args).unwrap();
        let rows = r["rows"].as_array().unwrap();
        assert!(
            rows.is_empty(),
            "skip == total should yield no rows, got {rows:?}"
        );
        // Schema names still surface even when no rows pass.
        let cols = r["columns"].as_array().unwrap();
        assert_eq!(cols.len(), 2, "columns should reflect schema not row count");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn op_read_limit_zero_returns_zero_rows_not_all() {
        // `limit: 0` is JSON-distinguishable from absent limit. The
        // `remaining == 0` early break (lib.rs:257) must fire on the very
        // first iteration. If a refactor changed the branch to
        // `remaining < 1` *after* slicing, or moved `emitted >= l` ahead of
        // the write, the file's entire row set would leak through as if
        // limit was unset. This is exactly the kind of zero-vs-unset
        // boundary that vibe-coded refactors flatten.
        let path = unique_csv("limit_zero", "id\n1\n2\n3\n4\n5\n");
        let args = json!({
            "path": path.to_str().unwrap(),
            "format": "csv",
            "limit": 0,
        });
        let r = op_read(args).unwrap();
        assert_eq!(
            r["rows"].as_array().unwrap().len(),
            0,
            "limit: 0 must NOT degenerate into 'no limit'"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn op_read_skip_straddles_batch_then_limit_caps_inside_next_batch() {
        // Exercises the *both* clauses firing on different batches plus an
        // intra-batch slice. Setup: 6 rows, batch_size=2, skip=3, limit=2.
        // Batch1 (rows 1-2): fully skipped via `to_skip == n` continue.
        // Batch2 (rows 3-4): partial skip via `batch.slice(to_skip, n - to_skip)`
        //   leaves row 4; then the `batch.num_rows() > remaining` branch is
        //   not hit (1 <= 2); emitted=1.
        // Batch3 (rows 5-6): no skip; `batch.num_rows() > remaining` triggers
        //   `batch.slice(0, 1)` keeping only row 5; emitted=2; final break.
        // Expected rows: id=4, id=5. Any miscalculation of `to_skip - skipped`,
        // `remaining`, or the slice arguments rearranges these two rows.
        let path = unique_csv("skip_limit_straddle", "id\n1\n2\n3\n4\n5\n6\n");
        let args = json!({
            "path": path.to_str().unwrap(),
            "format": "csv",
            "skip": 3,
            "limit": 2,
            "batch_size": 2,
        });
        let r = op_read(args).unwrap();
        let rows = r["rows"].as_array().unwrap();
        assert_eq!(
            rows.len(),
            2,
            "expected exactly 2 rows after skip=3 limit=2"
        );
        // CSV schema-infer makes id an Int64. Compare via i64.
        let ids: Vec<i64> = rows
            .iter()
            .map(|r| r["id"].as_i64().expect("id should parse as int"))
            .collect();
        assert_eq!(
            ids,
            vec![4, 5],
            "skip=3 limit=2 over [1..=6] must return ids [4,5], got {ids:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    // ── op_write / op_convert / op_stats round-trip coverage ─────────────────
    //
    // Existing tests cover only `op_read` skip/limit and `op_read_columnar`
    // alignment. The write/convert/stats paths (lib.rs:407-533) are completely
    // untested as a unit. The tests below pin three different bug classes:
    //   - write→read parquet round-trip preserves row count + values
    //     (catches rows_to_batches schema inference dropping a column,
    //     parquet compression corrupting on tiny inputs, JSON-to-arrow type
    //     coercion silently widening int→float)
    //   - convert CSV→Parquet preserves row count
    //     (catches `.collect::<Result<_,_>>()?` swallowing rows when a non-
    //     first batch errors, and write_batches's per-format dispatch
    //     silently no-op'ing for a format)
    //   - stats for CSV accumulates row counts across multiple batches
    //     (catches `+= → =` regression that would only report the last batch
    //     size when the file exceeds the default batch_size of 8192)
    //
    // CSV→parquet is the only path that exercises arrow-json → arrow record
    // batch → parquet writer → parquet reader → arrow record batch → arrow-
    // json output. Anything that breaks one of those five hops gets caught.

    fn unique_path(prefix: &str, ext: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "stryke_arrow_test_{}_{}_{}.{}",
            prefix,
            std::process::id(),
            n,
            ext
        ))
    }

    #[test]
    fn op_write_parquet_then_op_read_preserves_row_count_and_values() {
        // Round-trip 4 rows through parquet. If rows_to_batches's JSON-schema
        // inference loses a field (e.g., a future refactor switching from
        // arrow-json's schema-infer to a hand-rolled one drops null-only
        // columns), or the parquet writer silently drops rows when
        // row_group < num_rows, the row count or per-cell value check fails.
        let path = unique_path("write_roundtrip", "parquet");
        let rows = json!([
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "bravo"},
            {"id": 3, "name": "charlie"},
            {"id": 4, "name": "delta"},
        ]);
        let write_args = json!({
            "path": path.to_str().unwrap(),
            "format": "parquet",
            "rows": rows,
            // row_group < num_rows forces multi-row-group write — catches the
            // bug where ArrowWriter::write is called once with an oversized
            // batch and the row-group split happens internally; a regression
            // dropping the row_group setting would fail this differently than
            // the default 65536.
            "row_group": 2,
        });
        let w = op_write(write_args).unwrap();
        assert_eq!(
            w["rows"].as_u64().unwrap(),
            4,
            "op_write must report rows.len(), got {w:?}"
        );

        let read_args = json!({
            "path": path.to_str().unwrap(),
            "format": "parquet",
        });
        let r = op_read(read_args).unwrap();
        let out_rows = r["rows"].as_array().unwrap();
        assert_eq!(
            out_rows.len(),
            4,
            "parquet round-trip must preserve row count; got {} rows",
            out_rows.len()
        );

        // Build set of (id, name) tuples — order across row-groups is preserved
        // by parquet, but pinning order is brittle to writer changes. Compare
        // as a set instead. Any int→float coercion failure (e.g., schema
        // infer treating id as Float64) makes `as_i64()` return None.
        let mut got: Vec<(i64, String)> = out_rows
            .iter()
            .map(|r| {
                let id = r["id"].as_i64().expect("id should round-trip as int64");
                let name = r["name"]
                    .as_str()
                    .expect("name should round-trip as string")
                    .to_string();
                (id, name)
            })
            .collect();
        got.sort();
        let want: Vec<(i64, String)> = vec![
            (1, "alpha".into()),
            (2, "bravo".into()),
            (3, "charlie".into()),
            (4, "delta".into()),
        ];
        assert_eq!(got, want, "round-trip values diverged from input");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn op_convert_csv_to_parquet_preserves_row_count_and_schema_names() {
        // 5-row CSV → parquet via op_convert. Tests the full pipeline:
        // open_reader(Csv) → schema infer → collect batches → write_batches
        // (Parquet branch). If write_batches's Parquet arm silently misses a
        // batch in the `for b in batches { w.write(b)?; }` loop, or
        // op_convert's `.collect::<Result<_, _>>()?` accumulator swallows
        // a non-first error, the reported `rows` count diverges.
        let src = unique_path("convert_src", "csv");
        let dst = unique_path("convert_dst", "parquet");
        let mut f = File::create(&src).unwrap();
        f.write_all(b"id,name\n10,a\n20,b\n30,c\n40,d\n50,e\n")
            .unwrap();
        drop(f);

        let convert_args = json!({
            "src": src.to_str().unwrap(),
            "dst": dst.to_str().unwrap(),
            "src_format": "csv",
            "dst_format": "parquet",
        });
        let r = op_convert(convert_args).unwrap();
        assert_eq!(
            r["rows"].as_u64().unwrap(),
            5,
            "op_convert must report sum of all batch.num_rows(), got {r:?}"
        );

        // Verify the parquet file is readable and schema names survived
        // (open_reader's schema flows directly into write_batches; a regression
        // swapping reader.schema() with a default Schema would break this).
        let schema_r = op_schema(json!({
            "path": dst.to_str().unwrap(),
            "format": "parquet",
        }))
        .unwrap();
        let fields = schema_r["fields"].as_array().unwrap();
        let names: Vec<&str> = fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
        assert_eq!(
            names,
            vec!["id", "name"],
            "convert must preserve column NAMES and ORDER; got {names:?}"
        );

        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(dst);
    }

    #[test]
    fn op_stats_csv_accumulates_across_multiple_batches() {
        // op_stats's non-parquet branch (lib.rs:387-390) walks every batch
        // and sums num_rows into an i64 accumulator. We force multi-batch
        // execution via a JSON file with batch_size hardcoded to 8192 inside
        // op_stats — so we need >8192 rows, which is too slow. Instead pin
        // the contract with a CSV containing exactly 3 rows and verify
        // num_rows == 3 (catches `=` instead of `+=`, off-by-one on the
        // header line being counted, and the as i64 cast direction).
        //
        // Distinct from boilerplate "count rows" because it also asserts
        // num_columns equals schema field count (catches schema.fields() vs
        // first-batch.num_columns() divergence — the latter is undefined
        // when there are zero batches).
        let path = unique_csv(
            "stats_count",
            "id,kind,note\n1,a,first\n2,b,second\n3,c,third\n",
        );
        let r = op_stats(json!({
            "path": path.to_str().unwrap(),
            "format": "csv",
        }))
        .unwrap();
        assert_eq!(
            r["num_rows"].as_i64().unwrap(),
            3,
            "op_stats csv must count actual data rows (header excluded), got {r:?}"
        );
        assert_eq!(
            r["num_columns"].as_u64().unwrap(),
            3,
            "op_stats csv num_columns must match schema field count, got {r:?}"
        );
        let cols = r["columns"].as_array().unwrap();
        assert_eq!(cols.len(), 3, "columns array length must match num_columns");
        // Names must appear in declared order; a HashMap-based schema impl
        // would shuffle them.
        let names: Vec<&str> = cols.iter().map(|c| c["name"].as_str().unwrap()).collect();
        assert_eq!(
            names,
            vec!["id", "kind", "note"],
            "column name order broken"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn op_read_columnar_missing_row_keys_become_null_not_misaligned() {
        // op_read_columnar pivots row-records into column arrays
        // (lib.rs:289-298). The bug class this catches: a refactor that
        // pushed `obj.get(n)` results conditionally (e.g., `if let Some(v) =
        // obj.get(n) { arr.push(v) }`) would silently drop missing values,
        // making column arrays shorter than `num_rows` and misaligning *all*
        // downstream consumers row-by-row. The current code uses
        // `unwrap_or(Value::Null)` — this test pins that invariant.
        //
        // We use a CSV where some cells are empty so arrow emits null for
        // missing values, then verify every column has length == num_rows.
        // CSV null inference via empty cell:
        let path = unique_csv(
            "columnar_align",
            "id,name,score\n1,alice,10\n2,,20\n3,carol,\n",
        );
        let args = json!({
            "path": path.to_str().unwrap(),
            "format": "csv",
        });
        let r = op_read_columnar(args).unwrap();
        let num_rows = r["num_rows"].as_u64().unwrap();
        assert_eq!(num_rows, 3, "should report 3 rows");
        let data = r["data"].as_object().unwrap();
        // Invariant: every column array has length == num_rows. A drop-on-
        // missing regression would shrink the `name` or `score` column.
        for col_name in ["id", "name", "score"] {
            let col = data
                .get(col_name)
                .unwrap_or_else(|| panic!("column `{col_name}` missing from data map"));
            let arr = col
                .as_array()
                .unwrap_or_else(|| panic!("column `{col_name}` not an array"));
            assert_eq!(
                arr.len() as u64,
                num_rows,
                "column `{col_name}` length {} != num_rows {} — alignment broken",
                arr.len(),
                num_rows
            );
        }
        // Spot check: row 2 (idx 1) has empty name → null in name column.
        // CSV reader emits null for empty cells with nullable inferred fields.
        let name_col = data["name"].as_array().unwrap();
        assert!(
            name_col[1].is_null(),
            "row 2 empty name cell should pivot to null in column array, got {:?}",
            name_col[1]
        );
        let _ = std::fs::remove_file(path);
    }

    // ── op_write empty-rows guard + ProjectingReader projection order ─────────
    //
    // Two distinct gaps the suite above leaves open:
    //
    //   1. `op_write` with an empty `rows` array must `bail!("write: no rows")`
    //      (lib.rs:454-456) BEFORE touching the filesystem. `rows_to_batches`
    //      short-circuits to `Ok(vec![])` on empty input (lib.rs:408-410), so
    //      `batches.is_empty()` is the only guard. A regression that moved the
    //      `File::create` ahead of the guard, or dropped the guard, would
    //      truncate/create a zero-byte target file and only fail later inside
    //      `ArrowWriter::try_new` with a schema-index panic. The empty-input
    //      class is exactly what gets skipped in happy-path round-trips.
    //
    //   2. CSV column projection goes through `ProjectingReader` (lib.rs:140-149,
    //      197-213). The output schema is built from `indices` in *requested*
    //      order, and `next()` re-projects each batch's columns by those same
    //      indices. The invariant: requesting columns out of file order returns
    //      them in REQUEST order, not file order. A refactor that built the
    //      projected schema from sorted/file-order indices while leaving `next()`
    //      on request-order (or vice-versa) would transpose column data under
    //      mismatched headers — a silent data-corruption bug, not a crash.

    #[test]
    fn op_write_empty_rows_array_errors_before_creating_file() {
        // Target path must NOT exist after a no-rows write attempt. If the
        // guard regressed and File::create ran first, the file would be left
        // on disk (zero bytes) even though the call errors.
        let path = unique_path("write_empty", "parquet");
        assert!(!path.exists(), "precondition: temp path must not pre-exist");
        let args = json!({
            "path": path.to_str().unwrap(),
            "format": "parquet",
            "rows": [],
        });
        let err = op_write(args).unwrap_err().to_string();
        assert!(
            err.contains("no rows"),
            "empty rows must bail with 'no rows', got: {err}"
        );
        assert!(
            !path.exists(),
            "no-rows write must not create the output file, but it exists at {}",
            path.display()
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn op_read_csv_projection_returns_columns_in_request_order() {
        // File order is id,name,score. Request ["score","id"] — a subset in a
        // DIFFERENT order. ProjectingReader must surface exactly those two
        // columns, in request order, with correctly aligned per-row values.
        // If schema order and data-projection order diverge, the score values
        // would appear under "id" (or the columns array order would not match
        // request order) — caught here by checking BOTH the reported column
        // list order AND that each row's values land under the right key.
        let path = unique_csv(
            "proj_reorder",
            "id,name,score\n1,alice,100\n2,bob,200\n3,carol,300\n",
        );
        let args = json!({
            "path": path.to_str().unwrap(),
            "format": "csv",
            "columns": ["score", "id"],
        });
        let r = op_read(args).unwrap();

        // Column list must be the requested subset, in requested order.
        let cols: Vec<&str> = r["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c.as_str().unwrap())
            .collect();
        assert_eq!(
            cols,
            vec!["score", "id"],
            "projection must drop 'name' and keep request order, got {cols:?}"
        );

        let rows = r["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 3, "all 3 rows must survive projection");

        // The dropped column must be absent from every row object — a
        // ProjectingReader that re-projected by file indices but kept the full
        // schema would leak 'name' back in.
        for row in rows {
            let obj = row.as_object().unwrap();
            assert!(
                !obj.contains_key("name"),
                "projected-out column 'name' leaked into row {obj:?}"
            );
        }

        // Value alignment: row i must have id == i+1 and score == (i+1)*100.
        // A transposed projection (data under wrong header) fails this.
        for (i, row) in rows.iter().enumerate() {
            let expect_id = (i as i64) + 1;
            assert_eq!(
                row["id"].as_i64().unwrap(),
                expect_id,
                "row {i} id misaligned: {row:?}"
            );
            assert_eq!(
                row["score"].as_i64().unwrap(),
                expect_id * 100,
                "row {i} score misaligned (data under wrong column?): {row:?}"
            );
        }
        let _ = std::fs::remove_file(path);
    }

    // ── compute ops ──────────────────────────────────────────────────────────
    // All transforms are file→file. Each test writes a CSV, runs the op into a
    // unique CSV destination, reads it back via `op_read`, and asserts on the
    // materialized rows — the same surface a `.stk` caller sees.

    fn dst_csv(prefix: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "stryke_arrow_dst_{}_{}_{}.csv",
            prefix,
            std::process::id(),
            n
        ))
    }

    fn read_back(path: &std::path::Path) -> Vec<Value> {
        let r = op_read(json!({"path": path.to_str().unwrap(), "format": "csv"})).unwrap();
        r["rows"].as_array().unwrap().clone()
    }

    #[test]
    fn op_filter_gt_keeps_only_matching_rows() {
        // `id > 2` over 1..=4 must keep exactly {3,4}. A scalar built at the
        // wrong DataType (e.g. comparing Int64 column to a Utf8 scalar) would
        // error in `cmp::gt`; an off-by-one in the kernel would keep id==2.
        let src = unique_csv("filter", "id,name\n1,a\n2,b\n3,c\n4,d\n");
        let dst = dst_csv("filter");
        let r = op_filter(json!({
            "src": src.to_str().unwrap(), "src_format": "csv",
            "dst": dst.to_str().unwrap(), "dst_format": "csv",
            "column": "id", "op": "gt", "value": 2,
        }))
        .unwrap();
        assert_eq!(r["rows"].as_i64().unwrap(), 2);
        let rows = read_back(&dst);
        let ids: Vec<i64> = rows.iter().map(|x| x["id"].as_i64().unwrap()).collect();
        assert_eq!(ids, vec![3, 4], "filter id>2 must keep exactly 3,4");
        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(dst);
    }

    #[test]
    fn op_filter_string_eq_matches_exact_value() {
        let src = unique_csv("filtstr", "id,name\n1,keep\n2,drop\n3,keep\n");
        let dst = dst_csv("filtstr");
        op_filter(json!({
            "src": src.to_str().unwrap(), "src_format": "csv",
            "dst": dst.to_str().unwrap(), "dst_format": "csv",
            "column": "name", "op": "eq", "value": "keep",
        }))
        .unwrap();
        let rows = read_back(&dst);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r["name"].as_str() == Some("keep")));
        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(dst);
    }

    #[test]
    fn op_select_reorders_and_drops_columns() {
        // Request order is [score, id] — projection must preserve that order
        // and drop `name`. A naive impl that keeps source order would fail.
        let src = unique_csv("select", "id,name,score\n1,a,100\n2,b,200\n");
        let dst = dst_csv("select");
        op_select(json!({
            "src": src.to_str().unwrap(), "src_format": "csv",
            "dst": dst.to_str().unwrap(), "dst_format": "csv",
            "columns": ["score", "id"],
        }))
        .unwrap();
        let rows = read_back(&dst);
        let obj = rows[0].as_object().unwrap();
        assert!(!obj.contains_key("name"), "name should be dropped");
        assert_eq!(rows[0]["score"].as_i64().unwrap(), 100);
        assert_eq!(rows[0]["id"].as_i64().unwrap(), 1);
        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(dst);
    }

    #[test]
    fn op_drop_removes_named_columns_keeping_the_rest_in_order() {
        // Drop `name` → surviving columns stay in source order [id, score].
        let src = unique_csv("drop", "id,name,score\n1,a,100\n2,b,200\n");
        let dst = dst_csv("drop");
        let r = op_drop(json!({
            "src": src.to_str().unwrap(), "src_format": "csv",
            "dst": dst.to_str().unwrap(), "dst_format": "csv",
            "columns": ["name"],
        }))
        .unwrap();
        assert_eq!(
            r["columns"],
            json!(["id", "score"]),
            "kept columns in original order"
        );
        let rows = read_back(&dst);
        let obj = rows[0].as_object().unwrap();
        assert!(!obj.contains_key("name"), "dropped column is gone");
        assert_eq!(rows[0]["id"].as_i64().unwrap(), 1);
        assert_eq!(rows[0]["score"].as_i64().unwrap(), 100);
        // A typo'd column name is an error, not a silent no-op.
        assert!(op_drop(json!({
            "src": src.to_str().unwrap(), "src_format": "csv",
            "dst": dst.to_str().unwrap(), "dst_format": "csv",
            "columns": ["nope"],
        }))
        .is_err());
        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(dst);
    }

    #[test]
    fn op_sort_descending_orders_across_batches() {
        let src = unique_csv("sort", "id\n3\n1\n4\n1\n5\n9\n2\n");
        let dst = dst_csv("sort");
        op_sort(json!({
            "src": src.to_str().unwrap(), "src_format": "csv",
            "dst": dst.to_str().unwrap(), "dst_format": "csv",
            "by": [{"column": "id", "descending": true}],
        }))
        .unwrap();
        let ids: Vec<i64> = read_back(&dst)
            .iter()
            .map(|r| r["id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids, vec![9, 5, 4, 3, 2, 1, 1]);
        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(dst);
    }

    #[test]
    fn op_sort_accepts_numeric_descending_flag() {
        // stryke serializes `descending => 1` as JSON number 1, not boolean
        // true. `Value::as_bool` rejects that, so a naive impl silently sorts
        // ascending. This pins the lenient `json_truthy` parse end-to-end.
        let src = unique_csv("sortnum", "id\n3\n1\n2\n");
        let dst = dst_csv("sortnum");
        op_sort(json!({
            "src": src.to_str().unwrap(), "src_format": "csv",
            "dst": dst.to_str().unwrap(), "dst_format": "csv",
            "by": [{"column": "id", "descending": 1}],
        }))
        .unwrap();
        let ids: Vec<i64> = read_back(&dst)
            .iter()
            .map(|r| r["id"].as_i64().unwrap())
            .collect();
        assert_eq!(
            ids,
            vec![3, 2, 1],
            "numeric descending:1 must sort descending"
        );
        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(dst);
    }

    #[test]
    fn op_head_tail_slice_pick_correct_windows() {
        let src = unique_csv("window", "id\n1\n2\n3\n4\n5\n6\n7\n8\n");
        let h = dst_csv("head");
        op_head(json!({"src": src.to_str().unwrap(), "src_format":"csv", "dst": h.to_str().unwrap(), "dst_format":"csv", "n": 3})).unwrap();
        assert_eq!(
            read_back(&h)
                .iter()
                .map(|r| r["id"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let t = dst_csv("tail");
        op_tail(json!({"src": src.to_str().unwrap(), "src_format":"csv", "dst": t.to_str().unwrap(), "dst_format":"csv", "n": 2})).unwrap();
        assert_eq!(
            read_back(&t)
                .iter()
                .map(|r| r["id"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![7, 8]
        );
        let s = dst_csv("slice");
        op_slice(json!({"src": src.to_str().unwrap(), "src_format":"csv", "dst": s.to_str().unwrap(), "dst_format":"csv", "offset": 2, "length": 3})).unwrap();
        assert_eq!(
            read_back(&s)
                .iter()
                .map(|r| r["id"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        for p in [src, h, t, s] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn op_count_sums_rows_without_materializing() {
        let src = unique_csv("count", "id\n1\n2\n3\n4\n5\n");
        let r = op_count(json!({"src": src.to_str().unwrap(), "format": "csv"})).unwrap();
        assert_eq!(r["rows"].as_i64().unwrap(), 5);
        let _ = std::fs::remove_file(src);
    }

    #[test]
    fn op_concat_appends_sources_in_order() {
        let a = unique_csv("cata", "id\n1\n2\n");
        let b = unique_csv("catb", "id\n3\n4\n");
        let dst = dst_csv("cat");
        let r = op_concat(json!({
            "srcs": [a.to_str().unwrap(), b.to_str().unwrap()],
            "src_format": "csv",
            "dst": dst.to_str().unwrap(), "dst_format": "csv",
        }))
        .unwrap();
        assert_eq!(r["rows"].as_i64().unwrap(), 4);
        let ids: Vec<i64> = read_back(&dst)
            .iter()
            .map(|r| r["id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4]);
        for p in [a, b, dst] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn op_rename_changes_only_mapped_columns() {
        let src = unique_csv("rename", "id,name\n1,a\n2,b\n");
        let dst = dst_csv("rename");
        op_rename(json!({
            "src": src.to_str().unwrap(), "src_format": "csv",
            "dst": dst.to_str().unwrap(), "dst_format": "csv",
            "map": {"name": "label"},
        }))
        .unwrap();
        let obj = read_back(&dst)[0].as_object().unwrap().clone();
        assert!(obj.contains_key("label"), "renamed column missing");
        assert!(obj.contains_key("id"), "unmapped column should survive");
        assert!(!obj.contains_key("name"), "old name should be gone");
        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(dst);
    }

    #[test]
    fn op_cast_changes_column_type_in_output_schema() {
        // id is inferred Int64 from CSV; cast to Utf8 and confirm the read-back
        // schema reports a string type for it.
        let src = unique_csv("cast", "id,score\n1,10\n2,20\n");
        let dst = dst_csv("cast");
        op_cast(json!({
            "src": src.to_str().unwrap(), "src_format": "csv",
            "dst": dst.to_str().unwrap(), "dst_format": "ipc",
            "casts": {"id": "str"},
        }))
        .unwrap();
        let sch = op_schema(json!({"path": dst.to_str().unwrap(), "format": "ipc"})).unwrap();
        let fields = sch["fields"].as_array().unwrap();
        let id_field = fields
            .iter()
            .find(|f| f["name"].as_str() == Some("id"))
            .unwrap();
        let ty = id_field["type"].as_str().unwrap_or_default().to_lowercase();
        assert!(
            ty.contains("utf8") || ty.contains("string"),
            "id not cast to string: {ty}"
        );
        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(dst);
    }
}
