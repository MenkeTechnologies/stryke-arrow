#![allow(clippy::doc_lazy_continuation)]
//! Round 4 integration tests for the stryke-arrow-helper CLI:
//!   - `--help` exits 0 and prints the "Arrow" / format-list summary
//!   - `--version` exits 0 and prints a semver-shaped string
//!   - Invalid subcommand exits with code 2 (clap convention)
//!   - Missing required `path` on `read` exits non-zero with usage hint
//!   - `convert` subcommand requires BOTH `src` AND `dst` positionals
//!   - Empty argv (no subcommand) exits non-zero with usage on stderr
//!
//! Earlier rounds (in `#[cfg(test)] mod tests` inside `src/main.rs`) pinned:
//!   - Fmt::parse / detect / from_override_or_path
//!   - parse_columns shape (None / split / trim / empty filtering)
//!   - Cmd::Read / ReadColumnar / Schema / Stats / Write / Convert routing
//!       + required-path assertions + default-value assertions for skip / batch_size /
//!       row_group / compression / infer_bytes
//!
//! These tests target the OUTER process-level contracts (exit codes,
//! stderr/stdout framing, --help / --version conventions) that the
//! pure-function CLI tests inside src/ can't reach.

use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_stryke-arrow-helper"))
}

/// `--help` exits 0 and writes a summary mentioning "Arrow" to stdout.
/// Pin process-level help conventions.
#[test]
fn test_help_flag_exits_zero_and_summarises_arrow_in_stdout() {
    let out = bin().arg("--help").output().expect("spawn --help");
    assert!(out.status.success(), "--help must exit 0");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Arrow") || stdout.contains("arrow"),
        "--help must include the word 'Arrow'; got {stdout:?}"
    );
    assert!(
        stdout.to_lowercase().contains("usage"),
        "--help must include 'Usage:'; got {stdout:?}"
    );
}

/// `--version` exits 0 and prints semver-shaped output (X.Y.Z).
#[test]
fn test_version_flag_exits_zero_and_prints_semver_string() {
    let out = bin().arg("--version").output().expect("spawn --version");
    assert!(out.status.success(), "--version must exit 0");
    let stdout = String::from_utf8_lossy(&out.stdout);
    // Must contain at least one X.Y.Z token.
    let has_semver = stdout.split_whitespace().any(|tok| {
        let parts: Vec<&str> = tok.split('.').collect();
        parts.len() >= 3 && parts.iter().all(|p| p.chars().all(|c| c.is_ascii_digit()))
    });
    assert!(
        has_semver,
        "--version must include a semver-shaped X.Y.Z token; got {stdout:?}"
    );
}

/// Invalid subcommand exits with code 2 (clap's MissingRequired / Unknown).
#[test]
fn test_unknown_subcommand_exits_with_code_two() {
    let out = bin()
        .arg("definitely-not-a-real-cmd")
        .output()
        .expect("spawn");
    assert!(
        !out.status.success(),
        "unknown subcommand must NOT exit 0; got {:?}",
        out.status
    );
    assert_eq!(
        out.status.code(),
        Some(2),
        "clap convention: unknown subcommand → exit 2; got {:?}",
        out.status.code()
    );
}

/// `read` without the required path positional exits non-zero AND prints a
/// usage hint to stderr.
#[test]
fn test_read_subcommand_missing_path_exits_nonzero_with_usage_hint() {
    let out = bin().arg("read").output().expect("spawn read (no path)");
    assert!(
        !out.status.success(),
        "read without path must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.to_lowercase().contains("usage") || stderr.contains("--help"),
        "missing-path error must include usage hint; got {stderr:?}"
    );
}

/// `convert` requires BOTH src and dst — passing only one is rejected.
#[test]
fn test_convert_with_only_src_no_dst_is_rejected() {
    let out = bin()
        .args(["convert", "/tmp/src.parquet"])
        .output()
        .expect("spawn convert (src only)");
    assert!(
        !out.status.success(),
        "convert with only src must exit non-zero"
    );
}

/// Empty argv (no subcommand) is rejected with usage info on stderr.
#[test]
fn test_no_subcommand_exits_nonzero_with_stderr_usage() {
    let out = bin().output().expect("spawn (no args)");
    assert!(
        !out.status.success(),
        "no subcommand must exit non-zero; got {:?}",
        out.status
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.to_lowercase().contains("usage") || stderr.contains("--help"),
        "no-subcommand error must mention usage or --help; got {stderr:?}"
    );
}
