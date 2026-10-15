//! `brisk-parquet`: the archive's and the recorder's fast Parquet work.
//!
//!     brisk-parquet verify DIRECTORY [--row-group ROWS] [--threads N] [--format FORMAT]
//!     brisk-parquet schemas DIRECTORY
//!
//! `verify` prints the recording's summary as JSON, and `progress: ...` lines to stderr as each table is done. A recording
//! that breaks a rule exits 2 with `refused: MESSAGE` on stderr; any other failure exits 1.
use std::path::PathBuf;
use std::process::ExitCode;

use brisk_parquet::error::Failure;
use brisk_parquet::verify::{Options, verify_broker};

// Row groups are decoded and freed over and over; musl's allocator, the archive's build target, hands the memory back to the
// system each time.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn usage() -> ExitCode {
    eprintln!(
        "usage: brisk-parquet verify DIRECTORY [--row-group ROWS] [--threads N] [--format FORMAT] | schemas DIRECTORY"
    );
    ExitCode::from(64)
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("verify") => {
            let mut options = Options::default();
            let mut directory = None;
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--row-group" => match args.next().and_then(|v| v.parse().ok()) {
                        Some(rows) => options.row_group = rows,
                        None => return usage(),
                    },
                    "--format" => match args.next() {
                        Some(format) => options.format = Some(format),
                        None => return usage(),
                    },
                    "--threads" => match args.next().and_then(|v| v.parse().ok()) {
                        Some(threads) => options.threads = threads,
                        None => return usage(),
                    },
                    other if directory.is_none() && !other.starts_with("--") => {
                        directory = Some(PathBuf::from(other))
                    }
                    _ => return usage(),
                }
            }
            let Some(directory) = directory else {
                return usage();
            };
            let progress = |label: &str| eprintln!("progress: {label}");
            match verify_broker(&directory, &options, &progress) {
                Ok(summary) => {
                    println!("{summary}");
                    ExitCode::SUCCESS
                }
                Err(Failure::Refused(message)) => {
                    eprintln!("refused: {message}");
                    ExitCode::from(2)
                }
                Err(Failure::Fault(error)) => {
                    eprintln!("error: {error:#}");
                    ExitCode::from(1)
                }
            }
        }
        Some("schemas") => match args.next() {
            Some(directory) => match brisk_parquet::writer::empty(&PathBuf::from(directory)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("error: {error}");
                    ExitCode::from(1)
                }
            },
            None => usage(),
        },
        _ => usage(),
    }
}
