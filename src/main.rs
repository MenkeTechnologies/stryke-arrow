//! `stryke-arrow-helper` — bridge binary that gives the stryke `arrow` package
//! Parquet / Arrow IPC / Feather / CSV / JSON capability without bloating the
//! stryke core binary.
//!
//! Protocol (kept deliberately simple so the stryke side is `qx`/`open` glue):
//!
//! * `read`           → NDJSON to stdout, one row per line.
//! * `read-columnar`  → single JSON object `{schema, columns}` to stdout.
//! * `schema`         → single JSON object describing the file's schema.
//! * `stats`          → single JSON object with row count + per-column stats
//!                      (Parquet uses footer metadata; other formats scan).
//! * `write`          → reads NDJSON from stdin, writes the requested format.
//! * `convert`        → server-side conversion without round-tripping through
//!                      the stryke process.
//!
//! Format is auto-detected from the path extension; pass `--format` to override.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatchReader;
use arrow_csv::reader::Format as CsvFormat;
use arrow_csv::ReaderBuilder as CsvReaderBuilder;
use arrow_csv::WriterBuilder as CsvWriterBuilder;
use arrow_ipc::reader::FileReader as IpcFileReader;
use arrow_ipc::writer::FileWriter as IpcFileWriter;
use arrow_json::reader::ReaderBuilder as JsonReaderBuilder;
use arrow_json::writer::{LineDelimited, WriterBuilder as JsonWriterBuilder};
use clap::{Parser, Subcommand};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use serde::Serialize;
use serde_json::{json, Value};

/// Format the helper knows how to read/write.
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
            .ok_or_else(|| anyhow!("no extension on `{}` — pass --format", path.display()))?;
        Fmt::parse(ext)
    }

    fn from_override_or_path(override_name: Option<&str>, path: &Path) -> Result<Fmt> {
        if let Some(name) = override_name {
            Fmt::parse(name)
        } else {
            Fmt::detect(path)
        }
    }
}

#[derive(Parser)]
#[command(
    name = "stryke-arrow-helper",
    version,
    about = "Arrow / Parquet / IPC / Feather / CSV / JSON bridge for the stryke `arrow` package"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Stream rows as NDJSON to stdout.
    Read {
        path: PathBuf,
        #[arg(long)]
        format: Option<String>,
        /// Comma-separated column projection (case-sensitive).
        #[arg(long)]
        columns: Option<String>,
        /// Maximum rows to emit.
        #[arg(long)]
        limit: Option<usize>,
        /// Rows to skip before emitting.
        #[arg(long, default_value_t = 0)]
        skip: usize,
        #[arg(long, default_value_t = 8192)]
        batch_size: usize,
    },
    /// Dump the file as a single columnar JSON object.
    ReadColumnar {
        path: PathBuf,
        #[arg(long)]
        format: Option<String>,
        #[arg(long)]
        columns: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Print the schema (single JSON object).
    Schema {
        path: PathBuf,
        #[arg(long)]
        format: Option<String>,
    },
    /// Print row count + per-column stats. Parquet uses footer metadata; other
    /// formats scan.
    Stats {
        path: PathBuf,
        #[arg(long)]
        format: Option<String>,
    },
    /// Consume NDJSON from stdin and write to PATH.
    Write {
        path: PathBuf,
        #[arg(long)]
        format: Option<String>,
        /// snappy | gzip | zstd | lz4 | brotli | uncompressed (parquet only).
        #[arg(long, default_value = "snappy")]
        compression: String,
        #[arg(long, default_value_t = 65536)]
        row_group: usize,
        /// Path to a JSON schema spec (skip inference for huge inputs).
        #[arg(long)]
        schema: Option<PathBuf>,
        /// How many bytes to buffer for schema inference (NDJSON path).
        #[arg(long, default_value_t = 1 << 20)]
        infer_bytes: usize,
    },
    /// Convert SRC → DST without going through the stryke process.
    Convert {
        src: PathBuf,
        dst: PathBuf,
        #[arg(long)]
        src_format: Option<String>,
        #[arg(long)]
        dst_format: Option<String>,
        #[arg(long, default_value = "snappy")]
        compression: String,
        #[arg(long, default_value_t = 65536)]
        row_group: usize,
    },
}

fn main() {
    if let Err(e) = run() {
        eprintln!("stryke-arrow-helper: {e:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Read {
            path,
            format,
            columns,
            limit,
            skip,
            batch_size,
        } => cmd_read(&path, format.as_deref(), columns.as_deref(), limit, skip, batch_size),
        Cmd::ReadColumnar {
            path,
            format,
            columns,
            limit,
        } => cmd_read_columnar(&path, format.as_deref(), columns.as_deref(), limit),
        Cmd::Schema { path, format } => cmd_schema(&path, format.as_deref()),
        Cmd::Stats { path, format } => cmd_stats(&path, format.as_deref()),
        Cmd::Write {
            path,
            format,
            compression,
            row_group,
            schema,
            infer_bytes,
        } => cmd_write(
            &path,
            format.as_deref(),
            &compression,
            row_group,
            schema.as_deref(),
            infer_bytes,
        ),
        Cmd::Convert {
            src,
            dst,
            src_format,
            dst_format,
            compression,
            row_group,
        } => cmd_convert(
            &src,
            &dst,
            src_format.as_deref(),
            dst_format.as_deref(),
            &compression,
            row_group,
        ),
    }
}

/* ------------------------------------------------------------------------- */
/* readers                                                                   */
/* ------------------------------------------------------------------------- */

/// Open the file and return a boxed RecordBatchReader that yields the file's
/// contents in chunks. Column projection is applied at the source format
/// where supported (parquet, ipc) to avoid materializing dropped columns.
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
            let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?
                .with_batch_size(batch_size);
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
            // CSV needs schema inference. Open twice: once to infer, once to read.
            let mut probe = File::open(path)
                .with_context(|| format!("opening csv `{}`", path.display()))?;
            let (inferred, _records_read) = CsvFormat::default()
                .with_header(true)
                .infer_schema(&mut probe, Some(1024))?;
            let schema = Arc::new(inferred);
            let file = File::open(path)?;
            let mut builder = CsvReaderBuilder::new(schema.clone())
                .with_header(true)
                .with_batch_size(batch_size);
            if let Some(cols) = columns {
                let indices = column_indices(&schema, cols)?;
                builder = builder.with_projection(indices);
            }
            Ok(Box::new(builder.build(file)?))
        }
        Fmt::Json => {
            // JSON: infer from a buffered prefix of the file (rewindable since
            // it's an on-disk file, not a pipe). arrow-json doesn't expose a
            // builder-level projection, so when columns are requested we wrap
            // the inner reader in a projecting adapter.
            let mut probe = BufReader::new(File::open(path)?);
            let (inferred, _) = arrow_json::reader::infer_json_schema_from_seekable(
                &mut probe,
                Some(1024),
            )?;
            let schema = Arc::new(inferred);
            let file = BufReader::new(File::open(path)?);
            let inner = JsonReaderBuilder::new(schema.clone())
                .with_batch_size(batch_size)
                .build(file)?;
            if let Some(cols) = columns {
                let indices = column_indices(&schema, cols)?;
                Ok(Box::new(ProjectingReader::new(Box::new(inner), indices)?))
            } else {
                Ok(Box::new(inner))
            }
        }
    }
}

/// Wraps a `RecordBatchReader` and projects each batch down to a subset of
/// columns. Used for formats whose builder doesn't expose push-down projection
/// (currently arrow-json).
struct ProjectingReader {
    inner: Box<dyn RecordBatchReader + Send>,
    indices: Vec<usize>,
    schema: SchemaRef,
}

impl ProjectingReader {
    fn new(inner: Box<dyn RecordBatchReader + Send>, indices: Vec<usize>) -> Result<Self> {
        let src = inner.schema();
        let fields: Vec<Field> = indices
            .iter()
            .map(|i| src.field(*i).clone())
            .collect();
        let schema = Arc::new(Schema::new(fields));
        Ok(Self { inner, indices, schema })
    }
}

impl Iterator for ProjectingReader {
    type Item = std::result::Result<RecordBatch, arrow::error::ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|r| r.and_then(|b| b.project(&self.indices)))
    }
}

impl RecordBatchReader for ProjectingReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn column_indices(schema: &Schema, columns: &[String]) -> Result<Vec<usize>> {
    let mut indices = Vec::with_capacity(columns.len());
    for name in columns {
        let idx = schema
            .index_of(name)
            .with_context(|| format!("column `{name}` not in schema"))?;
        indices.push(idx);
    }
    Ok(indices)
}

fn parse_columns(spec: Option<&str>) -> Option<Vec<String>> {
    spec.map(|s| {
        s.split(',')
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect()
    })
}

/* ------------------------------------------------------------------------- */
/* `read` — NDJSON streaming                                                 */
/* ------------------------------------------------------------------------- */

fn cmd_read(
    path: &Path,
    format: Option<&str>,
    columns: Option<&str>,
    limit: Option<usize>,
    skip: usize,
    batch_size: usize,
) -> Result<()> {
    let fmt = Fmt::from_override_or_path(format, path)?;
    let cols = parse_columns(columns);
    let mut reader = open_reader(path, fmt, cols.as_deref(), batch_size)?;

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    let mut writer = JsonWriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, LineDelimited>(&mut out);

    let mut emitted: usize = 0;
    let mut skipped: usize = 0;
    while let Some(batch) = reader.next() {
        let batch = batch?;
        let n = batch.num_rows();
        let mut batch = batch;
        if skipped < skip {
            let to_skip = (skip - skipped).min(n);
            skipped += to_skip;
            if to_skip == n {
                continue;
            }
            batch = batch.slice(to_skip, n - to_skip);
        }
        let remaining = limit.map(|l| l.saturating_sub(emitted)).unwrap_or(usize::MAX);
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
    out.flush()?;
    Ok(())
}

/* ------------------------------------------------------------------------- */
/* `read-columnar`                                                           */
/* ------------------------------------------------------------------------- */

fn cmd_read_columnar(
    path: &Path,
    format: Option<&str>,
    columns: Option<&str>,
    limit: Option<usize>,
) -> Result<()> {
    let fmt = Fmt::from_override_or_path(format, path)?;
    let cols = parse_columns(columns);
    let mut reader = open_reader(path, fmt, cols.as_deref(), 8192)?;
    let schema = reader.schema();

    // Accumulate every batch up to `limit` rows, then convert each column to
    // a serde_json::Value array via arrow-json's row writer.
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut total: usize = 0;
    while let Some(batch) = reader.next() {
        let mut batch = batch?;
        if let Some(l) = limit {
            let remaining = l.saturating_sub(total);
            if remaining == 0 {
                break;
            }
            if batch.num_rows() > remaining {
                batch = batch.slice(0, remaining);
            }
        }
        total += batch.num_rows();
        batches.push(batch);
        if limit.is_some_and(|l| total >= l) {
            break;
        }
    }

    // Build columnar JSON. For each column, walk all batches and emit a flat
    // JSON array. We round-trip through arrow-json's row writer for typed
    // value conversion, then pivot.
    let mut columnar: HashMap<String, Vec<Value>> = HashMap::new();
    for f in schema.fields() {
        columnar.insert(f.name().clone(), Vec::with_capacity(total));
    }

    for batch in &batches {
        let mut row_buf: Vec<u8> = Vec::with_capacity(batch.num_rows() * 64);
        {
            let mut row_writer = JsonWriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, LineDelimited>(&mut row_buf);
            row_writer.write(batch)?;
            row_writer.finish()?;
        }
        for line in row_buf.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let mut row: serde_json::Map<String, Value> = serde_json::from_slice(line)?;
            for f in schema.fields() {
                let v = row.remove(f.name()).unwrap_or(Value::Null);
                columnar.get_mut(f.name()).unwrap().push(v);
            }
        }
    }

    let schema_json = schema_to_json(&schema);
    let columns_obj: serde_json::Map<String, Value> = schema
        .fields()
        .iter()
        .map(|f| {
            let v = columnar.remove(f.name()).unwrap_or_default();
            (f.name().clone(), Value::Array(v))
        })
        .collect();
    let out = json!({
        "schema": schema_json,
        "num_rows": total,
        "columns": Value::Object(columns_obj),
    });
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut w, &out)?;
    w.write_all(b"\n")?;
    Ok(())
}

/* ------------------------------------------------------------------------- */
/* `schema`                                                                  */
/* ------------------------------------------------------------------------- */

fn cmd_schema(path: &Path, format: Option<&str>) -> Result<()> {
    let fmt = Fmt::from_override_or_path(format, path)?;
    let schema = load_schema(path, fmt)?;
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut w, &schema_to_json(&schema))?;
    w.write_all(b"\n")?;
    Ok(())
}

fn load_schema(path: &Path, fmt: Fmt) -> Result<SchemaRef> {
    match fmt {
        Fmt::Parquet => {
            let file = File::open(path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            Ok(builder.schema().clone())
        }
        Fmt::Ipc | Fmt::Feather => {
            let file = File::open(path)?;
            let reader = IpcFileReader::try_new(file, None)?;
            Ok(reader.schema())
        }
        Fmt::Csv => {
            let mut file = File::open(path)?;
            let (s, _) = CsvFormat::default()
                .with_header(true)
                .infer_schema(&mut file, Some(1024))?;
            Ok(Arc::new(s))
        }
        Fmt::Json => {
            let mut probe = BufReader::new(File::open(path)?);
            let (s, _) = arrow_json::reader::infer_json_schema_from_seekable(&mut probe, Some(1024))?;
            Ok(Arc::new(s))
        }
    }
}

#[derive(Serialize)]
struct FieldJson {
    name: String,
    #[serde(rename = "type")]
    ty: String,
    nullable: bool,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    metadata: HashMap<String, String>,
}

fn schema_to_json(schema: &Schema) -> Value {
    let fields: Vec<FieldJson> = schema.fields().iter().map(|f| field_to_json(f)).collect();
    let metadata: HashMap<String, String> = schema
        .metadata()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    json!({
        "fields": fields,
        "metadata": metadata,
    })
}

fn field_to_json(f: &Field) -> FieldJson {
    FieldJson {
        name: f.name().clone(),
        ty: data_type_label(f.data_type()),
        nullable: f.is_nullable(),
        metadata: f
            .metadata()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

/// Stable, lowercase type name. Falls back to Debug for unusual cases.
fn data_type_label(t: &DataType) -> String {
    match t {
        DataType::Null => "null".into(),
        DataType::Boolean => "bool".into(),
        DataType::Int8 => "int8".into(),
        DataType::Int16 => "int16".into(),
        DataType::Int32 => "int32".into(),
        DataType::Int64 => "int64".into(),
        DataType::UInt8 => "uint8".into(),
        DataType::UInt16 => "uint16".into(),
        DataType::UInt32 => "uint32".into(),
        DataType::UInt64 => "uint64".into(),
        DataType::Float16 => "float16".into(),
        DataType::Float32 => "float32".into(),
        DataType::Float64 => "float64".into(),
        DataType::Utf8 => "string".into(),
        DataType::LargeUtf8 => "large_string".into(),
        DataType::Utf8View => "string_view".into(),
        DataType::Binary => "binary".into(),
        DataType::LargeBinary => "large_binary".into(),
        DataType::BinaryView => "binary_view".into(),
        DataType::FixedSizeBinary(n) => format!("fixed_size_binary({n})"),
        DataType::Date32 => "date32".into(),
        DataType::Date64 => "date64".into(),
        DataType::Time32(u) => format!("time32({u:?})").to_lowercase(),
        DataType::Time64(u) => format!("time64({u:?})").to_lowercase(),
        DataType::Timestamp(u, tz) => match tz {
            Some(tz) => format!("timestamp({u:?},{tz})").to_lowercase(),
            None => format!("timestamp({u:?})").to_lowercase(),
        },
        DataType::Duration(u) => format!("duration({u:?})").to_lowercase(),
        DataType::Interval(i) => format!("interval({i:?})").to_lowercase(),
        DataType::Decimal128(p, s) => format!("decimal128({p},{s})"),
        DataType::Decimal256(p, s) => format!("decimal256({p},{s})"),
        DataType::List(f) => format!("list<{}>", data_type_label(f.data_type())),
        DataType::LargeList(f) => format!("large_list<{}>", data_type_label(f.data_type())),
        DataType::FixedSizeList(f, n) => format!("fixed_size_list<{},{n}>", data_type_label(f.data_type())),
        DataType::Struct(fields) => {
            let inner: Vec<String> = fields
                .iter()
                .map(|f| format!("{}:{}", f.name(), data_type_label(f.data_type())))
                .collect();
            format!("struct<{}>", inner.join(","))
        }
        DataType::Map(f, _) => format!("map<{}>", data_type_label(f.data_type())),
        other => format!("{other:?}").to_lowercase(),
    }
}

/* ------------------------------------------------------------------------- */
/* `stats`                                                                   */
/* ------------------------------------------------------------------------- */

fn cmd_stats(path: &Path, format: Option<&str>) -> Result<()> {
    let fmt = Fmt::from_override_or_path(format, path)?;
    let file_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let stats = match fmt {
        Fmt::Parquet => parquet_stats(path)?,
        _ => scan_stats(path, fmt)?,
    };
    let out = json!({
        "path": path.display().to_string(),
        "format": format_label(fmt),
        "file_size": file_size,
        "num_rows": stats.num_rows,
        "num_columns": stats.columns.len(),
        "columns": stats.columns,
    });
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut w, &out)?;
    w.write_all(b"\n")?;
    Ok(())
}

fn format_label(f: Fmt) -> &'static str {
    match f {
        Fmt::Parquet => "parquet",
        Fmt::Ipc => "ipc",
        Fmt::Feather => "feather",
        Fmt::Csv => "csv",
        Fmt::Json => "json",
    }
}

#[derive(Serialize, Default)]
struct ColStat {
    name: String,
    #[serde(rename = "type")]
    ty: String,
    nullable: bool,
    null_count: Option<i64>,
    distinct_count: Option<i64>,
    min: Option<Value>,
    max: Option<Value>,
}

struct StatsOut {
    num_rows: i64,
    columns: Vec<ColStat>,
}

fn parquet_stats(path: &Path) -> Result<StatsOut> {
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let meta = reader.metadata();
    let file_meta = meta.file_metadata();
    let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
        file_meta.schema_descr(),
        file_meta.key_value_metadata(),
    )?;
    let num_rows = file_meta.num_rows();

    let mut cols: Vec<ColStat> = arrow_schema
        .fields()
        .iter()
        .map(|f| ColStat {
            name: f.name().clone(),
            ty: data_type_label(f.data_type()),
            nullable: f.is_nullable(),
            null_count: Some(0),
            distinct_count: None,
            min: None,
            max: None,
        })
        .collect();

    // Aggregate per-row-group statistics: sum null counts, take min of mins
    // and max of maxes (compared as strings — Parquet stores them as
    // type-specific byte buffers and the round-trip through the arrow-json
    // value is what the consumer wants).
    for rg in 0..meta.num_row_groups() {
        let rg_meta = meta.row_group(rg);
        for (i, col) in cols.iter_mut().enumerate() {
            if i >= rg_meta.num_columns() {
                continue;
            }
            let col_meta = rg_meta.column(i);
            if let Some(s) = col_meta.statistics() {
                let nc = stats_null_count(s) as i64;
                col.null_count = Some(col.null_count.unwrap_or(0) + nc);
                if let Some(distinct) = stats_distinct_count(s) {
                    col.distinct_count = Some(col.distinct_count.unwrap_or(0) + distinct as i64);
                }
                if let Some(min) = stats_min_value(s) {
                    col.min = Some(merge_min(col.min.take(), min));
                }
                if let Some(max) = stats_max_value(s) {
                    col.max = Some(merge_max(col.max.take(), max));
                }
            }
        }
    }

    Ok(StatsOut { num_rows, columns: cols })
}

fn stats_null_count(s: &Statistics) -> u64 {
    s.null_count_opt().unwrap_or(0)
}

fn stats_distinct_count(s: &Statistics) -> Option<u64> {
    s.distinct_count_opt()
}

fn stats_min_value(s: &Statistics) -> Option<Value> {
    match s {
        Statistics::Boolean(v) => v.min_opt().map(|x| json!(x)),
        Statistics::Int32(v) => v.min_opt().map(|x| json!(x)),
        Statistics::Int64(v) => v.min_opt().map(|x| json!(x)),
        Statistics::Float(v) => v.min_opt().map(|x| json!(x)),
        Statistics::Double(v) => v.min_opt().map(|x| json!(x)),
        Statistics::ByteArray(v) => v.min_opt().map(|x| json!(String::from_utf8_lossy(x.data()))),
        Statistics::FixedLenByteArray(v) => {
            v.min_opt().map(|x| json!(String::from_utf8_lossy(x.data())))
        }
        Statistics::Int96(_) => None,
    }
}

fn stats_max_value(s: &Statistics) -> Option<Value> {
    match s {
        Statistics::Boolean(v) => v.max_opt().map(|x| json!(x)),
        Statistics::Int32(v) => v.max_opt().map(|x| json!(x)),
        Statistics::Int64(v) => v.max_opt().map(|x| json!(x)),
        Statistics::Float(v) => v.max_opt().map(|x| json!(x)),
        Statistics::Double(v) => v.max_opt().map(|x| json!(x)),
        Statistics::ByteArray(v) => v.max_opt().map(|x| json!(String::from_utf8_lossy(x.data()))),
        Statistics::FixedLenByteArray(v) => {
            v.max_opt().map(|x| json!(String::from_utf8_lossy(x.data())))
        }
        Statistics::Int96(_) => None,
    }
}

fn merge_min(prev: Option<Value>, candidate: Value) -> Value {
    match prev {
        None => candidate,
        Some(p) => {
            if compare_values(&candidate, &p) == std::cmp::Ordering::Less {
                candidate
            } else {
                p
            }
        }
    }
}

fn merge_max(prev: Option<Value>, candidate: Value) -> Value {
    match prev {
        None => candidate,
        Some(p) => {
            if compare_values(&candidate, &p) == std::cmp::Ordering::Greater {
                candidate
            } else {
                p
            }
        }
    }
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(xf), Some(yf)) => xf.partial_cmp(&yf).unwrap_or(Equal),
            _ => Equal,
        },
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        _ => Equal,
    }
}

/// Fallback stats path: open the file as a RecordBatchReader, scan once to
/// compute row count and per-column null counts. min/max left null.
fn scan_stats(path: &Path, fmt: Fmt) -> Result<StatsOut> {
    let mut reader = open_reader(path, fmt, None, 8192)?;
    let schema = reader.schema();
    let mut cols: Vec<ColStat> = schema
        .fields()
        .iter()
        .map(|f| ColStat {
            name: f.name().clone(),
            ty: data_type_label(f.data_type()),
            nullable: f.is_nullable(),
            null_count: Some(0),
            distinct_count: None,
            min: None,
            max: None,
        })
        .collect();
    let mut num_rows: i64 = 0;
    while let Some(batch) = reader.next() {
        let batch = batch?;
        num_rows += batch.num_rows() as i64;
        for (i, col) in batch.columns().iter().enumerate() {
            if let Some(c) = cols.get_mut(i) {
                let nc = col.null_count() as i64;
                c.null_count = Some(c.null_count.unwrap_or(0) + nc);
            }
        }
    }
    Ok(StatsOut { num_rows, columns: cols })
}

/* ------------------------------------------------------------------------- */
/* `write` — NDJSON in → file out                                            */
/* ------------------------------------------------------------------------- */

fn cmd_write(
    path: &Path,
    format: Option<&str>,
    compression: &str,
    row_group: usize,
    schema_override: Option<&Path>,
    infer_bytes: usize,
) -> Result<()> {
    let fmt = Fmt::from_override_or_path(format, path)?;

    // Buffer stdin so we can infer the schema on a prefix and then re-feed
    // it to the JSON decoder.
    let mut all_bytes: Vec<u8> = Vec::with_capacity(infer_bytes.min(64 * 1024));
    io::stdin().read_to_end(&mut all_bytes)?;

    let schema = if let Some(p) = schema_override {
        Arc::new(load_user_schema(p)?)
    } else {
        let mut probe = std::io::Cursor::new(&all_bytes);
        let (inferred, _) =
            arrow_json::reader::infer_json_schema_from_seekable(&mut probe, None)?;
        Arc::new(inferred)
    };

    let mut decoder = JsonReaderBuilder::new(schema.clone())
        .with_batch_size(row_group.min(8192))
        .build_decoder()?;

    // Push the buffered bytes through the decoder, then drain batches into
    // the format writer.
    decoder.decode(&all_bytes)?;
    drop(all_bytes);

    let mut sink: Box<dyn BatchSink> = open_writer(path, fmt, schema.clone(), compression, row_group)?;
    while let Some(batch) = decoder.flush()? {
        sink.write(&batch)?;
    }
    sink.finish()?;
    Ok(())
}

fn load_user_schema(path: &Path) -> Result<Schema> {
    let raw = std::fs::read_to_string(path)?;
    let parsed: serde_json::Value = serde_json::from_str(&raw)?;
    let fields_v = parsed
        .get("fields")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("schema file missing `fields` array"))?;
    let mut fields = Vec::with_capacity(fields_v.len());
    for f in fields_v {
        let name = f
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("schema field missing `name`"))?;
        let ty = f
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("schema field `{name}` missing `type`"))?;
        let nullable = f.get("nullable").and_then(|v| v.as_bool()).unwrap_or(true);
        fields.push(Field::new(name, parse_type_label(ty)?, nullable));
    }
    Ok(Schema::new(fields))
}

fn parse_type_label(t: &str) -> Result<DataType> {
    Ok(match t {
        "null" => DataType::Null,
        "bool" | "boolean" => DataType::Boolean,
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" | "int" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float16" => DataType::Float16,
        "float32" | "float" => DataType::Float32,
        "float64" | "double" => DataType::Float64,
        "string" | "utf8" | "str" => DataType::Utf8,
        "large_string" | "large_utf8" => DataType::LargeUtf8,
        "binary" => DataType::Binary,
        "large_binary" => DataType::LargeBinary,
        "date32" | "date" => DataType::Date32,
        "date64" => DataType::Date64,
        other => bail!("unknown type `{other}` in schema spec"),
    })
}

/* ------------------------------------------------------------------------- */
/* writers                                                                   */
/* ------------------------------------------------------------------------- */

trait BatchSink {
    fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<()>;
}

fn open_writer(
    path: &Path,
    fmt: Fmt,
    schema: SchemaRef,
    compression: &str,
    row_group: usize,
) -> Result<Box<dyn BatchSink>> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).ok();
        }
    }
    match fmt {
        Fmt::Parquet => {
            let file = File::create(path)?;
            let props = WriterProperties::builder()
                .set_compression(parse_compression(compression)?)
                .set_max_row_group_row_count(Some(row_group))
                .build();
            let writer = ArrowWriter::try_new(file, schema, Some(props))?;
            Ok(Box::new(ParquetSink { writer: Some(writer) }))
        }
        Fmt::Ipc | Fmt::Feather => {
            let file = File::create(path)?;
            let writer = IpcFileWriter::try_new(BufWriter::new(file), &schema)?;
            Ok(Box::new(IpcSink { writer: Some(writer) }))
        }
        Fmt::Csv => {
            let file = File::create(path)?;
            let writer = CsvWriterBuilder::new()
                .with_header(true)
                .build(BufWriter::new(file));
            Ok(Box::new(CsvSink { writer: Some(writer) }))
        }
        Fmt::Json => {
            let file = File::create(path)?;
            let inner = BufWriter::new(file);
            let writer = JsonWriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, LineDelimited>(inner);
            Ok(Box::new(JsonSink { writer: Some(writer) }))
        }
    }
}

fn parse_compression(name: &str) -> Result<Compression> {
    Ok(match name.to_ascii_lowercase().as_str() {
        "snappy" => Compression::SNAPPY,
        "gzip" | "gz" => Compression::GZIP(Default::default()),
        "zstd" => Compression::ZSTD(ZstdLevel::default()),
        "lz4" | "lz4_raw" => Compression::LZ4_RAW,
        "brotli" => Compression::BROTLI(Default::default()),
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        other => bail!("unknown compression `{other}`"),
    })
}

struct ParquetSink {
    writer: Option<ArrowWriter<File>>,
}
impl BatchSink for ParquetSink {
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.as_mut().unwrap().write(batch)?;
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> Result<()> {
        self.writer.take().unwrap().close()?;
        Ok(())
    }
}

struct IpcSink {
    writer: Option<IpcFileWriter<BufWriter<File>>>,
}
impl BatchSink for IpcSink {
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.as_mut().unwrap().write(batch)?;
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> Result<()> {
        self.writer.take().unwrap().finish()?;
        Ok(())
    }
}

struct CsvSink {
    writer: Option<arrow_csv::Writer<BufWriter<File>>>,
}
impl BatchSink for CsvSink {
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.as_mut().unwrap().write(batch)?;
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> Result<()> {
        drop(self.writer.take());
        Ok(())
    }
}

struct JsonSink {
    writer: Option<arrow_json::writer::Writer<BufWriter<File>, LineDelimited>>,
}
impl BatchSink for JsonSink {
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.as_mut().unwrap().write(batch)?;
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> Result<()> {
        let mut w = self.writer.take().unwrap();
        w.finish()?;
        Ok(())
    }
}

/* ------------------------------------------------------------------------- */
/* `convert`                                                                 */
/* ------------------------------------------------------------------------- */

fn cmd_convert(
    src: &Path,
    dst: &Path,
    src_format: Option<&str>,
    dst_format: Option<&str>,
    compression: &str,
    row_group: usize,
) -> Result<()> {
    let src_fmt = Fmt::from_override_or_path(src_format, src)?;
    let dst_fmt = Fmt::from_override_or_path(dst_format, dst)?;
    let mut reader = open_reader(src, src_fmt, None, 8192)?;
    let schema = reader.schema();
    let mut sink = open_writer(dst, dst_fmt, schema, compression, row_group)?;
    while let Some(batch) = reader.next() {
        let batch = batch?;
        sink.write(&batch)?;
    }
    sink.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── Fmt::parse / detect / from_override_or_path ─────────────────

    #[test]
    fn fmt_parse_accepts_all_known_aliases() {
        assert_eq!(Fmt::parse("parquet").unwrap(), Fmt::Parquet);
        assert_eq!(Fmt::parse("pq").unwrap(), Fmt::Parquet);
        assert_eq!(Fmt::parse("ipc").unwrap(), Fmt::Ipc);
        assert_eq!(Fmt::parse("arrow").unwrap(), Fmt::Ipc);
        assert_eq!(Fmt::parse("feather").unwrap(), Fmt::Feather);
        assert_eq!(Fmt::parse("csv").unwrap(), Fmt::Csv);
        assert_eq!(Fmt::parse("tsv").unwrap(), Fmt::Csv);
        assert_eq!(Fmt::parse("json").unwrap(), Fmt::Json);
        assert_eq!(Fmt::parse("ndjson").unwrap(), Fmt::Json);
        assert_eq!(Fmt::parse("jsonl").unwrap(), Fmt::Json);
    }

    #[test]
    fn fmt_parse_is_case_insensitive() {
        assert_eq!(Fmt::parse("PARQUET").unwrap(), Fmt::Parquet);
        assert_eq!(Fmt::parse("Json").unwrap(), Fmt::Json);
        assert_eq!(Fmt::parse("ARROW").unwrap(), Fmt::Ipc);
    }

    #[test]
    fn fmt_parse_unknown_errors() {
        let err = Fmt::parse("avro").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("unknown format"));
        assert!(msg.contains("avro"));
    }

    #[test]
    fn fmt_detect_uses_extension() {
        assert_eq!(Fmt::detect(Path::new("/tmp/x.parquet")).unwrap(), Fmt::Parquet);
        assert_eq!(Fmt::detect(Path::new("data.csv")).unwrap(), Fmt::Csv);
        assert_eq!(Fmt::detect(Path::new("a.b.c.ndjson")).unwrap(), Fmt::Json);
    }

    #[test]
    fn fmt_detect_no_extension_errors() {
        let err = Fmt::detect(Path::new("/tmp/noext")).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("no extension"), "msg = {msg}");
        assert!(msg.contains("--format"));
    }

    #[test]
    fn fmt_from_override_or_path_prefers_override() {
        // Path says csv but override says json — override wins.
        let f = Fmt::from_override_or_path(Some("json"), Path::new("/tmp/x.csv")).unwrap();
        assert_eq!(f, Fmt::Json);
    }

    #[test]
    fn fmt_from_override_or_path_falls_back_to_path() {
        let f = Fmt::from_override_or_path(None, Path::new("/tmp/x.parquet")).unwrap();
        assert_eq!(f, Fmt::Parquet);
    }

    // ─── parse_columns ───────────────────────────────────────────────

    #[test]
    fn parse_columns_none_passthrough() {
        assert_eq!(parse_columns(None), None);
    }

    #[test]
    fn parse_columns_splits_and_trims() {
        let got = parse_columns(Some(" a , b,c ")).unwrap();
        assert_eq!(got, vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_columns_filters_empty_segments() {
        // Trailing comma, double comma, leading comma — all dropped.
        let got = parse_columns(Some("a,,b,")).unwrap();
        assert_eq!(got, vec!["a", "b"]);
    }

    #[test]
    fn parse_columns_empty_string_returns_empty_vec() {
        // Distinct from None — caller passed --columns="", got Some([]).
        let got = parse_columns(Some(""));
        assert_eq!(got, Some(Vec::<String>::new()));
    }

    // ─── column_indices ──────────────────────────────────────────────

    #[test]
    fn column_indices_resolves_in_request_order() {
        let s = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, false),
        ]);
        let got = column_indices(&s, &["c".into(), "a".into()]).unwrap();
        assert_eq!(got, vec![2, 0]);
    }

    #[test]
    fn column_indices_missing_column_errors_with_name() {
        let s = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let err = column_indices(&s, &["nope".into()]).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("nope"), "msg = {msg}");
    }

    // ─── data_type_label ─────────────────────────────────────────────

    #[test]
    fn data_type_label_scalars_have_stable_lowercase_names() {
        assert_eq!(data_type_label(&DataType::Boolean), "bool");
        assert_eq!(data_type_label(&DataType::Int32), "int32");
        assert_eq!(data_type_label(&DataType::Int64), "int64");
        assert_eq!(data_type_label(&DataType::UInt8), "uint8");
        assert_eq!(data_type_label(&DataType::Float64), "float64");
        assert_eq!(data_type_label(&DataType::Utf8), "string");
        assert_eq!(data_type_label(&DataType::LargeUtf8), "large_string");
        assert_eq!(data_type_label(&DataType::Null), "null");
    }

    #[test]
    fn data_type_label_decimal_has_precision_and_scale() {
        assert_eq!(data_type_label(&DataType::Decimal128(20, 5)), "decimal128(20,5)");
        assert_eq!(data_type_label(&DataType::Decimal256(40, 10)), "decimal256(40,10)");
    }

    #[test]
    fn data_type_label_fixed_size_binary() {
        assert_eq!(data_type_label(&DataType::FixedSizeBinary(16)), "fixed_size_binary(16)");
    }

    #[test]
    fn data_type_label_list_nests_inner_type() {
        let inner = Field::new("item", DataType::Int32, true);
        let dt = DataType::List(Arc::new(inner));
        assert_eq!(data_type_label(&dt), "list<int32>");
    }

    #[test]
    fn data_type_label_struct_concatenates_fields() {
        let dt = DataType::Struct(
            vec![
                Field::new("x", DataType::Int32, false),
                Field::new("y", DataType::Utf8, true),
            ]
            .into(),
        );
        assert_eq!(data_type_label(&dt), "struct<x:int32,y:string>");
    }

    // ─── format_label ────────────────────────────────────────────────

    #[test]
    fn format_label_round_trips_via_parse() {
        for f in [Fmt::Parquet, Fmt::Ipc, Fmt::Feather, Fmt::Csv, Fmt::Json] {
            let label = format_label(f);
            assert_eq!(Fmt::parse(label).unwrap(), f, "label = {label}");
        }
    }

    // ─── parse_compression ───────────────────────────────────────────

    #[test]
    fn parse_compression_known_names() {
        assert_eq!(parse_compression("snappy").unwrap(), Compression::SNAPPY);
        assert_eq!(parse_compression("uncompressed").unwrap(), Compression::UNCOMPRESSED);
        assert_eq!(parse_compression("none").unwrap(), Compression::UNCOMPRESSED);
        assert_eq!(parse_compression("lz4_raw").unwrap(), Compression::LZ4_RAW);
        assert_eq!(parse_compression("lz4").unwrap(), Compression::LZ4_RAW);
    }

    #[test]
    fn parse_compression_case_insensitive() {
        assert_eq!(parse_compression("SNAPPY").unwrap(), Compression::SNAPPY);
        assert_eq!(parse_compression("Gzip").unwrap(), Compression::GZIP(Default::default()));
    }

    #[test]
    fn parse_compression_unknown_errors() {
        let err = parse_compression("bogus").unwrap_err();
        assert!(format!("{err}").contains("unknown compression"));
    }

    // ─── parse_type_label ────────────────────────────────────────────

    #[test]
    fn parse_type_label_aliases() {
        assert_eq!(parse_type_label("int").unwrap(), DataType::Int64);
        assert_eq!(parse_type_label("int64").unwrap(), DataType::Int64);
        assert_eq!(parse_type_label("float").unwrap(), DataType::Float32);
        assert_eq!(parse_type_label("double").unwrap(), DataType::Float64);
        assert_eq!(parse_type_label("str").unwrap(), DataType::Utf8);
        assert_eq!(parse_type_label("utf8").unwrap(), DataType::Utf8);
        assert_eq!(parse_type_label("string").unwrap(), DataType::Utf8);
        assert_eq!(parse_type_label("bool").unwrap(), DataType::Boolean);
        assert_eq!(parse_type_label("boolean").unwrap(), DataType::Boolean);
        assert_eq!(parse_type_label("date").unwrap(), DataType::Date32);
    }

    #[test]
    fn parse_type_label_unknown_errors_with_name() {
        let err = parse_type_label("foobar").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("foobar"));
    }

    // ─── compare_values / merge_min / merge_max ──────────────────────

    #[test]
    fn compare_values_numbers_use_f64_ordering() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!(1), &json!(2)), Less);
        assert_eq!(compare_values(&json!(2.5), &json!(2.5)), Equal);
        assert_eq!(compare_values(&json!(10), &json!(2)), Greater);
    }

    #[test]
    fn compare_values_mixed_types_return_equal() {
        // Defensive: differing JSON types fall through to Equal so the
        // merge functions don't change state when receiving a mismatched
        // candidate (e.g., a stringly-typed column hitting a numeric row).
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!(1), &json!("a")), Equal);
        assert_eq!(compare_values(&json!(true), &json!(1)), Equal);
        assert_eq!(compare_values(&Value::Null, &json!(1)), Equal);
    }

    #[test]
    fn merge_min_picks_smaller() {
        assert_eq!(merge_min(None, json!(5)), json!(5));
        assert_eq!(merge_min(Some(json!(10)), json!(5)), json!(5));
        assert_eq!(merge_min(Some(json!(2)), json!(7)), json!(2));
    }

    #[test]
    fn merge_max_picks_larger() {
        assert_eq!(merge_max(None, json!(5)), json!(5));
        assert_eq!(merge_max(Some(json!(10)), json!(5)), json!(10));
        assert_eq!(merge_max(Some(json!(2)), json!(7)), json!(7));
    }

    #[test]
    fn merge_min_strings_lex_order() {
        assert_eq!(merge_min(Some(json!("banana")), json!("apple")), json!("apple"));
        assert_eq!(merge_max(Some(json!("apple")), json!("banana")), json!("banana"));
    }

    // ─── load_user_schema (round-trip via on-disk JSON) ──────────────

    #[test]
    fn load_user_schema_missing_fields_array_errors() {
        let tmp = std::env::temp_dir().join(format!("stryke-arrow-test-{}.json", std::process::id()));
        std::fs::write(&tmp, r#"{"not_fields":[]}"#).unwrap();
        let err = load_user_schema(&tmp).unwrap_err();
        let _ = std::fs::remove_file(&tmp);
        assert!(format!("{err}").contains("fields"));
    }

    #[test]
    fn load_user_schema_field_without_name_errors() {
        let tmp = std::env::temp_dir().join(format!("stryke-arrow-test-{}-noname.json", std::process::id()));
        std::fs::write(&tmp, r#"{"fields":[{"type":"int"}]}"#).unwrap();
        let err = load_user_schema(&tmp).unwrap_err();
        let _ = std::fs::remove_file(&tmp);
        assert!(format!("{err}").contains("name"));
    }

    #[test]
    fn load_user_schema_happy_path_nullable_default_true() {
        let tmp = std::env::temp_dir().join(format!("stryke-arrow-test-{}-ok.json", std::process::id()));
        std::fs::write(
            &tmp,
            r#"{"fields":[
              {"name":"id","type":"int64","nullable":false},
              {"name":"name","type":"string"}
            ]}"#,
        )
        .unwrap();
        let s = load_user_schema(&tmp).unwrap();
        let _ = std::fs::remove_file(&tmp);
        assert_eq!(s.fields().len(), 2);
        assert_eq!(s.field(0).name(), "id");
        assert_eq!(s.field(0).data_type(), &DataType::Int64);
        assert!(!s.field(0).is_nullable());
        // Default nullable = true when omitted.
        assert!(s.field(1).is_nullable());
    }

    #[test]
    fn parse_compression_gzip_and_brotli() {
        assert!(matches!(parse_compression("gzip").unwrap(), Compression::GZIP(_)));
        assert!(matches!(parse_compression("brotli").unwrap(), Compression::BROTLI(_)));
    }

    #[test]
    fn parse_compression_zstd_default_level() {
        assert!(matches!(parse_compression("zstd").unwrap(), Compression::ZSTD(_)));
        assert!(matches!(parse_compression("ZSTD").unwrap(), Compression::ZSTD(_)));
    }

    #[test]
    fn parse_type_label_uint_and_float_aliases() {
        assert_eq!(parse_type_label("uint64").unwrap(), DataType::UInt64);
        assert_eq!(parse_type_label("float32").unwrap(), DataType::Float32);
    }

    #[test]
    fn data_type_label_timestamp_includes_unit_and_tz() {
        use arrow::datatypes::TimeUnit;
        let dt = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let label = data_type_label(&dt);
        assert!(label.contains("timestamp"));
        assert!(label.contains("utc"));
    }

    #[test]
    fn data_type_label_map_nests_value_type() {
        let entries = Field::new(
            "entries",
            DataType::Struct(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ].into()),
            false,
        );
        let dt = DataType::Map(Arc::new(entries), false);
        assert!(data_type_label(&dt).starts_with("map<"));
    }

    #[test]
    fn column_indices_empty_projection_returns_empty() {
        let s = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        assert!(column_indices(&s, &[]).unwrap().is_empty());
    }

    #[test]
    fn compare_values_bool_ordering() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!(false), &json!(true)), Less);
        assert_eq!(compare_values(&json!(true), &json!(true)), Equal);
    }

    #[test]
    fn schema_to_json_includes_field_metadata() {
        let mut meta = std::collections::HashMap::new();
        meta.insert("PARQUET:field_id".into(), "7".into());
        let f = Field::new("x", DataType::Int32, true).with_metadata(meta);
        let s = Schema::new(vec![f]);
        let j = schema_to_json(&s);
        assert_eq!(j["fields"][0]["name"], json!("x"));
        assert_eq!(j["fields"][0]["metadata"]["PARQUET:field_id"], json!("7"));
    }

    #[test]
    fn parse_type_label_int8_uint64_and_null() {
        assert_eq!(parse_type_label("int8").unwrap(), DataType::Int8);
        assert_eq!(parse_type_label("uint64").unwrap(), DataType::UInt64);
        assert_eq!(parse_type_label("null").unwrap(), DataType::Null);
    }

    #[test]
    fn parse_type_label_large_binary_alias() {
        assert_eq!(parse_type_label("large_binary").unwrap(), DataType::LargeBinary);
    }

    #[test]
    fn field_to_json_carries_nullable_flag() {
        let f = Field::new("n", DataType::Utf8, false);
        let j = field_to_json(&f);
        assert_eq!(j.name, "n");
        assert_eq!(j.ty, "string");
        assert!(!j.nullable);
    }

    #[test]
    fn compare_values_string_lexicographic() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!("apple"), &json!("banana")), Less);
    }

    #[test]
    fn merge_min_max_integer_json_numbers() {
        assert_eq!(merge_min(Some(json!(10)), json!(3)), json!(3));
        assert_eq!(merge_max(Some(json!(10)), json!(3)), json!(10));
    }

    #[test]
    fn fmt_detect_feather_extension() {
        assert_eq!(Fmt::detect(Path::new("x.feather")).unwrap(), Fmt::Feather);
    }

    #[test]
    fn load_user_schema_unknown_type_errors() {
        let tmp =
            std::env::temp_dir().join(format!("stryke-arrow-badtype-{}.json", std::process::id()));
        std::fs::write(&tmp, r#"{"fields":[{"name":"x","type":"avro"}]}"#).unwrap();
        let err = load_user_schema(&tmp).unwrap_err();
        let _ = std::fs::remove_file(&tmp);
        assert!(format!("{err}").contains("avro"));
    }

    #[test]
    fn data_type_label_uint16_and_float16() {
        assert_eq!(data_type_label(&DataType::UInt16), "uint16");
        assert_eq!(data_type_label(&DataType::Float16), "float16");
    }

    #[test]
    fn parse_type_label_date64_and_binary() {
        assert_eq!(parse_type_label("date64").unwrap(), DataType::Date64);
        assert_eq!(parse_type_label("binary").unwrap(), DataType::Binary);
    }

    #[test]
    fn format_label_all_formats_distinct() {
        let labels: Vec<_> = [Fmt::Parquet, Fmt::Ipc, Fmt::Feather, Fmt::Csv, Fmt::Json]
            .iter()
            .map(|f| format_label(*f))
            .collect();
        assert_eq!(labels.len(), 5);
        assert!(labels.iter().all(|l| !l.is_empty()));
    }

    #[test]
    fn column_indices_duplicate_column_name_uses_first() {
        let s = Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("x", DataType::Utf8, true),
        ]);
        let got = column_indices(&s, &["x".into()]).unwrap();
        assert_eq!(got, vec![0]);
    }

    #[test]
    fn compare_values_i64_json_number() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!(1), &json!(2i64)), Less);
    }

    #[test]
    fn merge_max_string_picks_lex_larger() {
        assert_eq!(merge_max(Some(json!("a")), json!("z")), json!("z"));
    }

    #[test]
    fn parse_columns_tabs_between_names() {
        let got = parse_columns(Some("a\t,\tb")).unwrap();
        assert_eq!(got, vec!["a", "b"]);
    }

    #[test]
    fn data_type_label_large_list_nests() {
        let inner = Field::new("item", DataType::Utf8, true);
        let dt = DataType::LargeList(Arc::new(inner));
        assert_eq!(data_type_label(&dt), "large_list<string>");
    }

    #[test]
    fn load_user_schema_missing_name_field_errors() {
        let tmp = std::env::temp_dir().join(format!("stryke-arrow-noname-{}.json", std::process::id()));
        std::fs::write(&tmp, r#"{"fields":[{"type":"int"}]}"#).unwrap();
        let err = load_user_schema(&tmp).unwrap_err();
        let _ = std::fs::remove_file(&tmp);
        assert!(format!("{err}").contains("name"));
    }

    #[test]
    fn parse_type_label_large_utf8_alias() {
        assert_eq!(parse_type_label("large_utf8").unwrap(), DataType::LargeUtf8);
    }

    #[test]
    fn data_type_label_duration_includes_unit() {
        use arrow::datatypes::TimeUnit;
        let dt = DataType::Duration(TimeUnit::Millisecond);
        let label = data_type_label(&dt);
        assert!(label.contains("duration"));
        assert!(label.contains("millisecond"));
    }

    #[test]
    fn fmt_parse_arrow_file_alias() {
        assert_eq!(Fmt::parse("arrow").unwrap(), Fmt::Ipc);
    }

    #[test]
    fn load_user_schema_field_missing_type_errors() {
        let tmp = std::env::temp_dir().join(format!("stryke-arrow-notype-{}.json", std::process::id()));
        std::fs::write(&tmp, r#"{"fields":[{"name":"x"}]}"#).unwrap();
        let err = load_user_schema(&tmp).unwrap_err();
        let _ = std::fs::remove_file(&tmp);
        assert!(format!("{err}").contains("type"));
    }

    #[test]
    fn merge_max_none_takes_candidate() {
        assert_eq!(merge_max(None, json!(99)), json!(99));
    }

    #[test]
    fn compare_values_array_vs_number_equal() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!([1]), &json!(1)), Equal);
    }

    #[test]
    fn schema_to_json_fields_array_len() {
        let s = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let j = schema_to_json(&s);
        assert_eq!(j["fields"].as_array().unwrap().len(), 2);
        assert_eq!(j["fields"][0]["name"], json!("a"));
    }

    #[test]
    fn parse_compression_brotli_case_insensitive() {
        assert!(matches!(parse_compression("Brotli").unwrap(), Compression::BROTLI(_)));
    }

    #[test]
    fn parse_type_label_int16_and_uint8() {
        assert_eq!(parse_type_label("int16").unwrap(), DataType::Int16);
        assert_eq!(parse_type_label("uint8").unwrap(), DataType::UInt8);
    }

    #[test]
    fn data_type_label_binary_and_large_binary() {
        assert_eq!(data_type_label(&DataType::Binary), "binary");
        assert_eq!(data_type_label(&DataType::LargeBinary), "large_binary");
    }

    #[test]
    fn fmt_detect_jsonl_extension() {
        assert_eq!(Fmt::detect(Path::new("events.jsonl")).unwrap(), Fmt::Json);
    }

    #[test]
    fn merge_min_equal_candidates_unchanged() {
        assert_eq!(merge_min(Some(json!(4)), json!(4)), json!(4));
    }

    #[test]
    fn compare_values_negative_numbers() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!(-10), &json!(-1)), Less);
    }

    #[test]
    fn parse_columns_spaces_around_names_trimmed() {
        assert_eq!(parse_columns(Some(" x , y ")).unwrap(), vec!["x", "y"]);
    }

    #[test]
    fn field_to_json_nullable_true_flag() {
        let f = Field::new("n", DataType::Utf8, true);
        assert!(field_to_json(&f).nullable);
    }

    #[test]
    fn fmt_from_override_or_path_unknown_override_still_wins() {
        let f = Fmt::from_override_or_path(Some("csv"), Path::new("x.parquet")).unwrap();
        assert_eq!(f, Fmt::Csv);
    }

    #[test]
    fn parse_type_label_uint16_and_float16() {
        assert_eq!(parse_type_label("uint16").unwrap(), DataType::UInt16);
        assert_eq!(parse_type_label("float16").unwrap(), DataType::Float16);
    }

    #[test]
    fn data_type_label_date32_and_date64() {
        assert_eq!(data_type_label(&DataType::Date32), "date32");
        assert_eq!(data_type_label(&DataType::Date64), "date64");
    }

    #[test]
    fn fmt_parse_tsv_alias() {
        assert_eq!(Fmt::parse("tsv").unwrap(), Fmt::Csv);
    }

    #[test]
    fn merge_max_equal_keeps_value() {
        assert_eq!(merge_max(Some(json!(7)), json!(7)), json!(7));
    }

    #[test]
    fn compare_values_null_to_null_equal() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&Value::Null, &Value::Null), Equal);
    }

    #[test]
    fn schema_to_json_empty_metadata_object() {
        let s = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
        let j = schema_to_json(&s);
        assert!(j["metadata"].is_object());
    }

    #[test]
    fn parse_compression_gzip_uppercase() {
        assert!(matches!(parse_compression("GZIP").unwrap(), Compression::GZIP(_)));
    }

    #[test]
    fn column_indices_single_column() {
        let s = Schema::new(vec![Field::new("only", DataType::Utf8, true)]);
        assert_eq!(column_indices(&s, &["only".into()]).unwrap(), vec![0]);
    }

    #[test]
    fn data_type_label_uint16() {
        assert_eq!(data_type_label(&DataType::UInt16), "uint16");
    }

    #[test]
    fn parse_columns_three_names() {
        assert_eq!(
            parse_columns(Some("a,b,c")).unwrap(),
            vec!["a", "b", "c"],
        );
    }

    #[test]
    fn merge_min_none_takes_candidate() {
        assert_eq!(merge_min(None, json!(5)), json!(5));
    }

    #[test]
    fn compare_values_number_less_than() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_values(&json!(1), &json!(9)), Less);
    }

    #[test]
    fn fmt_parse_jsonl_alias() {
        assert_eq!(Fmt::parse("jsonl").unwrap(), Fmt::Json);
    }

    #[test]
    fn schema_to_json_num_fields_matches() {
        let s = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        assert_eq!(schema_to_json(&s)["fields"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn parse_compression_snappy_lowercase() {
        assert!(matches!(parse_compression("snappy").unwrap(), Compression::SNAPPY));
    }

    #[test]
    fn column_indices_preserves_order() {
        let s = Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
        ]);
        assert_eq!(column_indices(&s, &["y".into(), "x".into()]).unwrap(), vec![1, 0]);
    }
}
