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
- [\[0x03\] CLI: `arrow`](#0x03-cli-arrow)
- [\[0x04\] API Reference](#0x04-api-reference)
- [\[0x05\] Helper Protocol](#0x05-helper-protocol)
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

```sh
cd ~/projects/stryke-arrow
cargo build --release           # produces target/release/stryke-arrow-helper
s pkg install -g .              # installs the `arrow` and `arrow-build` CLIs
```

Or the one-liner:

```sh
make install
```

After install, `arrow --help` works from anywhere on PATH (assuming
`~/.stryke/bin/` is on PATH). The stryke library is auto-discoverable to any
project that depends on the package via `[deps] arrow = { path = "..." }`
or, when published, by name.

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

## [0x03] CLI: `arrow`

```sh
arrow read    sales.parquet --columns=id,amount --limit=10
arrow head    sales.parquet 5
arrow schema  sales.parquet
arrow stats   sales.parquet
arrow rows    sales.parquet
arrow convert in.csv out.parquet --compression=zstd
arrow build                                  # build the helper via cargo
arrow version
```

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

### `Arrow::helper_path() → $path`
Returns the path to `stryke-arrow-helper`. Honors `$ENV{STRYKE_ARROW_HELPER}`.

### `Arrow::ensure_built() → $path`
Builds the helper via `cargo build --release` if missing.

### `Arrow::version() → $string`
Helper version string.

## [0x05] Helper Protocol

The helper speaks NDJSON over stdin/stdout. Useful if you want to skip
stryke entirely:

```sh
stryke-arrow-helper read   sales.parquet              # NDJSON rows to stdout
stryke-arrow-helper read-columnar sales.parquet       # one JSON object
stryke-arrow-helper schema sales.parquet
stryke-arrow-helper stats  sales.parquet
stryke-arrow-helper write  out.parquet < rows.ndjson  # NDJSON rows from stdin
stryke-arrow-helper convert in.csv out.parquet --compression=zstd
```

Format is detected from the file extension; pass `--format` to override.

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

The stryke library locates the helper in this order:

1. `$ENV{STRYKE_ARROW_HELPER}`
2. `<pkg>/target/release/stryke-arrow-helper`
3. `<pkg>/target/debug/stryke-arrow-helper`
4. `<pkg>/bin/stryke-arrow-helper`
5. `which stryke-arrow-helper`

`<pkg>` is derived from `__FILE__` on the loaded `Arrow.stk`, so it works
whether the package lives in your source tree or under `~/.stryke/store/`.

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
  Cargo.toml                   # Rust helper crate manifest
  Makefile                     # convenience targets
  src/main.rs                  # stryke-arrow-helper binary
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
