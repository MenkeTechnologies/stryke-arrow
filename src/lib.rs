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
        let path = unique_csv(
            "skip_limit_straddle",
            "id\n1\n2\n3\n4\n5\n6\n",
        );
        let args = json!({
            "path": path.to_str().unwrap(),
            "format": "csv",
            "skip": 3,
            "limit": 2,
            "batch_size": 2,
        });
        let r = op_read(args).unwrap();
        let rows = r["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2, "expected exactly 2 rows after skip=3 limit=2");
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
                arr.len() as u64, num_rows,
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
}
