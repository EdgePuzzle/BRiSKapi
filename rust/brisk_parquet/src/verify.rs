//! Verify a broker's recording (all four tables, in a directory) a row group at a time, never whole in memory
//! (`verify_broker` in `briskapi/_parquet.py` states the rules): exactly the expected tables, each row group the canonical
//! encoding of its content, the batches' rules, and of every quote and trade its identity, order, batch, clock and value
//! ranges, and its books' order. Not checked, the market's own consistency. Returns the summary the archive compares with
//! the manifest.
use std::path::Path;
use std::sync::Mutex;

use rayon::prelude::*;
use serde_json::{Value, json};

use crate::batches::{self, END, HISTORY, QUOTES};
use crate::error::{Failure, Res};
use crate::files;
use crate::master;
use crate::require;
use crate::rows::{Context, Group, Trades, check_depths};
use crate::schema::{self, ROW_GROUP};

pub struct Options {
    /// Rows of a full row group.
    pub row_group: usize,
    pub threads: usize,
    /// The format the tables must be in; the one their depths table says if none.
    pub format: Option<String>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            row_group: ROW_GROUP,
            threads: std::thread::available_parallelism().map_or(1, |n| n.get()),
            format: None,
        }
    }
}

const MAX_HISTORY: u64 = 50_000;

pub fn verify_broker(
    directory: &Path,
    options: &Options,
    progress: &(dyn Fn(&str) + Sync),
) -> Res<Value> {
    let format = match &options.format {
        Some(format) => Some(format.clone()),
        None => files::format_of(directory)?,
    };
    let legacy = match format.as_deref() {
        Some(schema::FORMAT) => false,
        Some(schema::LEGACY_FORMAT) => true,
        _ => return crate::error::refuse("Unexpected format"),
    };
    let tables: Vec<files::Table> = schema::FILES
        .iter()
        .map(|name| files::open(directory, name, options.row_group, legacy))
        .collect::<Res<_>>()?;
    let [equity, depths, trades, batch_table] = <[files::Table; 4]>::try_from(tables)
        .ok()
        .expect("four tables");
    let (equity_rows, master) = master::read(directory, &equity)?;
    let batches = batches::verify(directory, &batch_table, &master, legacy)?;
    master::check_canonical(&equity_rows, &master, batches.midnight)?;
    progress("verify: batches");
    let n = batches.kinds.len();
    let context = Context::new(&batches, &master, legacy);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(options.threads.max(1))
        .build()
        .map_err(|e| Failure::Fault(e.into()))?;

    // Quotes: row groups in parallel, each counting its quotes per batch into its thread's own array.
    let tallies: Vec<Mutex<Vec<u32>>> = (0..pool.current_num_threads())
        .map(|_| Mutex::new(vec![0u32; n]))
        .collect();
    let groups: Vec<Group> = pool.install(|| {
        (0..depths.groups)
            .into_par_iter()
            .map(|g| {
                let batch = depths.read(directory, g)?;
                let index = rayon::current_thread_index().unwrap_or(0);
                let mut counts = tallies[index].lock().unwrap_or_else(|e| e.into_inner());
                check_depths(&batch, &context, &mut counts)
            })
            .collect::<Res<Vec<Group>>>()
    })?;
    let mut previous: Option<&Group> = None;
    for group in &groups {
        if let Some(before) = previous {
            let (a, b) = (before.last, group.first);
            require!(
                (a.issue, a.seq) < (b.issue, b.seq),
                "Quotes not sorted by issue, then batch"
            );
            require!(
                a.issue != b.issue || (b.frame >= a.frame && b.time >= a.time),
                "Quote frame/time regression"
            );
        }
        previous = Some(group);
    }
    let quote_rows: usize = groups.iter().map(|g| g.rows).sum();
    require!(
        quote_rows == depths.rows,
        format!("Non-canonical {}", schema::DEPTHS)
    );
    let quote_bytes: u64 = groups.iter().map(|g| g.bytes).sum();
    let mut quotes = vec![0u64; n];
    for tally in &tallies {
        for (total, count) in quotes
            .iter_mut()
            .zip(tally.lock().unwrap_or_else(|e| e.into_inner()).iter())
        {
            *total += *count as u64;
        }
    }
    drop(tallies);
    progress("verify: depths");

    // Trades: in order, as their IDs count on from one row group to the next.
    let mut counts = vec![0u32; n];
    let mut state = Trades::default();
    for g in 0..trades.groups {
        let batch = trades.read(directory, g)?;
        state.check(&batch, &context, &mut counts)?;
    }
    require!(
        state.rows == trades.rows,
        format!("Non-canonical {}", schema::TRADES)
    );
    let trade_counts: Vec<u64> = counts.iter().map(|&c| c as u64).collect();
    progress("verify: trades");

    require!(
        quotes[0] + batches.first_skipped as u64 == master.len() as u64,
        "Incomplete bootstrap"
    );
    for i in 0..n {
        if batches.kinds[i] == HISTORY {
            require!(
                trade_counts[i] > 0 && trade_counts[i] <= MAX_HISTORY,
                "Invalid trades"
            );
        }
    }
    let updates: u64 = (0..n)
        .filter(|&i| batches.kinds[i] == QUOTES)
        .map(|i| quotes[i])
        .sum();
    require!(batches.end_quote_updates == updates, "End summary mismatch");
    let mut expanded = quote_bytes + state.bytes;
    for i in 0..n {
        expanded += batches.lengths[i] as u64;
        if batches.kinds[i] != END {
            expanded += trade_counts[i].saturating_sub(1);
        }
        if batches.kinds[i] <= QUOTES {
            expanded += quotes[i].saturating_sub(1);
        }
    }
    let mut codes: Vec<&str> = master.iter().map(|m| m.code.as_str()).collect();
    codes.sort_unstable();
    Ok(json!({
        "source": batches.source, "trading_date": batches.trading_date,
        "first_source_time_us": batches.first_source_time_us, "last_source_time_us": batches.last_source_time_us,
        "codes": codes, "batches": n, "quote_updates": quotes.iter().sum::<u64>(), "expanded_bytes": expanded,
    }))
}
