```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                   [ a r r o w ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-arrow/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-arrow/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[APACHE ARROW + PARQUET + FEATHER + ARROW-CSV/JSON // STRYKE PACKAGE]`

> *"Columnar data, on demand. No daily-driver weight."*

Apache Arrow + Parquet + Arrow IPC + Feather + arrow-CSV + arrow-JSON for stryke. Opt-in package, kept out of the stryke core binary so the daily-driver install stays slim. Created by MenkeTechnologies.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-parquet`](https://github.com/MenkeTechnologies/stryke-parquet) · [`stryke-duckdb`](https://github.com/MenkeTechnologies/stryke-duckdb) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Why a Package, Not a Builtin](#0x00-why-a-package-not-a-builtin)
- [\[0x01\] Install](#0x01-install)
- [\[0x02\] Quick Start](#0x02-quick-start)
- [\[0x03\] Options](#0x03-options)
- [\[0x04\] API Reference](#0x04-api-reference)
- [\[0x05\] FFI Layer](#0x05-ffi-layer)
- [\[0x06\] Supported Formats](#0x06-supported-formats)
- [\[0x07\] Compression](#0x07-compression)
- [\[0x08\] Discovery](#0x08-discovery)
- [\[0x09\] Tests](#0x09-tests)
- [\[0x0A\] Dev Workflow](#0x0a-dev-workflow)
- [\[0x0B\] Layout](#0x0b-layout)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Why a Package, Not a Builtin

stryke's core stays small on purpose — most one-liner / awk-replacement work
doesn't need 200 transitive crates of columnar data infrastructure. arrow-rs +
parquet hit a different scale:

| Tier | Properties | This package |
|---|---|---|
| Core builtins (~40 MB stryke) | small deps, used everywhere | string, math, regex, parallel ops, scipy-class math |
| Package tier (opt-in) | heavy deps, narrow use cases | parquet, arrow, big-ML, cloud SDKs, niche formats |

`stryke-arrow` ships as a local stryke package + a sidecar Rust binary
(`stryke-arrow-helper`) built from this repo. The stryke side is a thin
NDJSON-pipe wrapper; the heavy arrow-rs/parquet code lives in the helper and
is loaded on demand. Core stryke is never linked against arrow.

## [0x01] Install

From a release (no rustc on the consumer machine):

```sh
s pkg install -g github.com/MenkeTechnologies/stryke-arrow
```

From a local checkout (publisher / contributor workflow):

```sh
cd ~/projects/stryke-arrow
cargo build --release           # produces target/release/libstryke_arrow.{dylib,so}
s pkg install -g .              # installs into ~/.stryke/store/arrow@<version>/
```

Or:

```sh
make install
```

The cdylib is dlopened in-process on first `use Arrow` (stryke's FFI
bridge resolves the symbols at module load — no helper binary, no
subprocess fork per call).

## [0x02] Quick Start

```perl
use Arrow

# Read parquet/arrow/feather/csv/json — format detected from extension.
my @rows = Arrow::read("sales.parquet")
p $rows[0]

# Stream huge files without buffering.
Arrow::read_stream("events.parquet", callback => sub ($row) {
    process($row)
})

# Cheap metadata: footer-only for parquet/ipc.
my $sch = Arrow::schema("sales.parquet")
p "fields: " . join(", ", map { "$_->{name}:$_->{type}" } @{ $sch->{fields} })

p Arrow::row_count("sales.parquet")           # parquet footer, no scan

# Stats: row count + per-column null counts, min, max, distinct (parquet
# uses footer metadata, other formats scan once).
p to_json(Arrow::stats("sales.parquet"))

# Write — schema inferred from the first NDJSON batch.
Arrow::write("out.parquet", \@rows, compression => "zstd")

# Server-side conversion (never round-trips through stryke memory).
Arrow::convert("in.csv", "out.parquet", compression => "zstd")
```

Per-format aliases when you want it explicit:

```perl
use Arrow::Parquet
use Arrow::IPC
use Arrow::Feather
use Arrow::CSV
use Arrow::JSON

Arrow::Parquet::read("x.parquet")
Arrow::IPC::write("x.arrow", \@rows)
Arrow::CSV::stats("x.csv")
```

DataFrame bridge:

```perl
use Arrow::DataFrame

my $df = Arrow::DataFrame::load("sales.parquet")
# $df is a stryke DataFrame when the builtin is available; otherwise a
# { col => [vals] } columnar hash.
```

## [0x03] Options

Every `Arrow::*` op accepts `%opts`. Read fields:

```
format       → parquet|ipc|arrow|feather|csv|tsv|json|ndjson  (default: extension-detected)
columns      → \@names  — projection at the source format
limit        → max rows
skip         → rows to skip from the start
batch_size   → reader batch size (default 8192)
```

Write fields:

```
format       → as above
compression  → snappy|gzip|zstd|lz4|brotli|none  (parquet only; default snappy)
row_group    → max rows per parquet row group (default 65536)
```

Convert fields:

```
src_format, dst_format, compression, row_group
```

Sublibraries (`Arrow::Parquet`, `Arrow::IPC`, `Arrow::Feather`, `Arrow::CSV`,
`Arrow::JSON`, `Arrow::DataFrame`) pin `format` automatically — see
`lib/Parquet.stk` etc.

## [0x04] API Reference

### `Arrow::read(PATH, %opts) → @rows | \@rows`
Load every row as a hashref. Options: `format`, `columns` (array or comma
string), `limit`, `skip`, `batch_size`.

### `Arrow::read_stream(PATH, callback => sub ($row) { … }, %opts) → $count`
Same options as `read`; calls the callback once per row without buffering.

### `Arrow::read_columnar(PATH, %opts) → { schema, num_rows, columns }`
Single columnar object. Faster than `read` when you want column-major access.

### `Arrow::schema(PATH, %opts) → { fields, metadata }`
Schema only; no data scan.

### `Arrow::stats(PATH, %opts) → { num_rows, num_columns, file_size, columns }`
Parquet pulls min/max/null_count from row-group statistics. CSV/JSON/IPC scan
once. Each entry in `columns` is `{ name, type, nullable, null_count,
distinct_count, min, max }`.

### `Arrow::row_count(PATH, %opts) → $n`
Shortcut around `stats`.

### `Arrow::write(PATH, \@rows, %opts) → $n`
Options: `format`, `compression` (parquet only: `snappy|gzip|zstd|lz4|brotli|uncompressed`),
`row_group`, `schema` (path to a JSON schema spec to skip inference on huge
inputs).

### `Arrow::write_iter(PATH, sub { … } , %opts) → $n`
Iterator form: subref returns one row per call, `undef` to stop. Streams to
the helper without holding all rows in stryke.

### `Arrow::convert(SRC, DST, %opts) → DST`
Server-side reader-to-writer pipeline. Options: `src_format`, `dst_format`,
`compression`, `row_group`. Doesn't round-trip data through the stryke
process.

### `Arrow::version() → $string`
The cdylib's package version (`env!("CARGO_PKG_VERSION")`).

## [0x05] FFI Layer

Each `Arrow::*` wrapper builds a JSON args dict and calls a sibling
`arrow__*` symbol resolved out of `libstryke_arrow.{dylib,so}`. The cdylib
is dlopened in-process on first `use Arrow` (via stryke's
`pkg::commands::try_load_ffi_for` resolver hook) and exposes 7 entry
points: `version`, `read`, `read_columnar`, `schema`, `stats`, `write`,
`convert`.

Wire shape (cdylib responses):

* `read` → `{"columns": [...], "rows": [{col: val, ...}, ...]}`
* `read_columnar` → `{"columns": [...], "num_rows": N, "data": {col: [...]}}`
* `schema` → `{"fields": [{name, type, nullable}, ...]}`
* `stats` (parquet) → `{"num_rows", "num_row_groups", "columns": [{name, null_count, min, max}, ...]}`
* `write`, `convert` → `{"path": ..., "rows": N}`
* Errors → `{"error": "<msg>"}` — the wrapper `die`s with it

Stateless package — arrow operations are pure file transforms, no
process-level cache.

## [0x06] Supported Formats

| Format | Read | Write | Notes |
|---|---|---|---|
| Parquet | ✓ | ✓ | snappy / gzip / zstd / lz4 / brotli / uncompressed |
| Arrow IPC | ✓ | ✓ | `.arrow` extension; no compression |
| Feather | ✓ | ✓ | alias for Arrow IPC v2 |
| CSV | ✓ | ✓ | header row mandatory; schema inferred from first 1024 rows |
| JSON (NDJSON) | ✓ | ✓ | line-delimited only |

## [0x07] Compression

| Codec | Library | Notes |
|---|---|---|
| `snappy` | `snap` | default; fast, modest ratio |
| `zstd` | `zstd` | best ratio per CPU |
| `gzip` | `flate2` | broad compatibility |
| `lz4` | `lz4_flex` | LZ4_RAW frame |
| `brotli` | `brotli` | high ratio, slow |
| `uncompressed` | — | fastest write, biggest file |

## [0x08] Discovery

The cdylib lives next to `Arrow.stk` inside the package install dir:

```
~/.stryke/store/arrow@<version>/
  stryke.toml
  lib/
    Arrow.stk
    libstryke_arrow.{dylib,so}
```

On `use Arrow`, stryke's `pkg::commands::try_load_ffi_for` reads the
sibling `stryke.toml`, finds the `[ffi]` table, and dlopens the cdylib
once for the life of the process. No env vars, no PATH probing, no
helper-binary discovery — the install dir IS the discovery answer.

## [0x09] Tests

```sh
cargo test                       # helper CLI contract tests (tests/)
s test t/                        # end-to-end round-trip per format
```

`t/test_arrow.stk` writes a small dataset in every format, reads it back,
checks shape + values, and exits TAP-style.

## [0x0A] Dev Workflow

```sh
make             # release build
make debug       # faster compile
make test        # cargo test + s test t/
make install     # release + pkg install -g .
make clean
```

## [0x0B] Layout

```
stryke-arrow/
  stryke.toml                  # stryke package manifest
  Cargo.toml                   # cdylib crate manifest
  Makefile                     # convenience targets
  src/lib.rs                   # cdylib — arrow__* extern "C" exports
  lib/
    Arrow.stk                  # `use Arrow`
    Parquet.stk                # `use Arrow::Parquet`
    IPC.stk                    # `use Arrow::IPC`
    Feather.stk                # `use Arrow::Feather`
    CSV.stk                    # `use Arrow::CSV`
    JSON.stk                   # `use Arrow::JSON`
    DataFrame.stk              # `use Arrow::DataFrame`
  t/
    test_arrow.stk             # round-trip tests per format
  examples/
    read_parquet.stk
    csv_to_parquet.stk
    dataframe_bridge.stk
```

## [0xFF] License

MIT.
