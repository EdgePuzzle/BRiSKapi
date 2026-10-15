//! The master (the equity table): the securities a recording holds, decoded and checked the way the archive does
//! (`_decode_equity`, `_check_master`) and compared with the table its content implies.
use std::path::Path;

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, TimestampNanosecondType, UInt8Type, UInt64Type};
use arrow_array::{Array, RecordBatch, StringArray};
use serde_json::{Value, json};

use crate::error::Res;
use crate::files::Table;
use crate::require;
use crate::units::{TWO_53, tenths};

#[derive(Clone, Debug)]
pub struct Master {
    pub issue_id: u64,
    pub code: String,
    pub tick_type: u64,
    pub base_price10: u64,
    pub limit_up10: u64,
    pub limit_down10: u64,
    pub lot_size: u64,
    pub issue_type: u64,
    pub name: String,
}

impl Master {
    /// The master entry as the decoder's canonical JSON has it.
    pub fn json(&self) -> Value {
        json!({"issue_id": self.issue_id, "code": self.code, "tick_type": self.tick_type, "base_price10": self.base_price10,
               "limit_up10": self.limit_up10, "limit_down10": self.limit_down10, "lot_size": self.lot_size,
               "issue_type": self.issue_type, "name": self.name})
    }
}

/// A security code: 4 to 8 of A-Z and 0-9.
pub fn valid_code(code: &str) -> bool {
    (4..=8).contains(&code.len())
        && code
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

fn text<'a>(batch: &'a RecordBatch, name: &str) -> Res<&'a StringArray> {
    match batch
        .column_by_name(name)
        .and_then(|c| c.as_string_opt::<i32>())
    {
        Some(a) => Ok(a),
        None => crate::error::refuse(format!("Unexpected schema in {name}")),
    }
}

/// A price text as the master prints it ("1234.5", none for zero), and back.
pub fn price_text(tenths: u64) -> Option<String> {
    (tenths != 0).then(|| format!("{}.{}", tenths / 10, tenths % 10))
}

fn price_tenths(text: Option<&str>) -> Res<u64> {
    let Some(text) = text else { return Ok(0) };
    let valid = text.split_once('.').is_some_and(|(whole, tenth)| {
        (1..=16).contains(&whole.len())
            && whole.bytes().all(|b| b.is_ascii_digit())
            && tenth.len() == 1
            && tenth.bytes().all(|b| b.is_ascii_digit())
    });
    require!(valid, "Invalid price text");
    let (whole, tenth) = text.split_once('.').unwrap();
    Ok(whole
        .parse::<u64>()
        .unwrap_or(u64::MAX)
        .saturating_mul(10)
        .saturating_add(tenth.parse::<u64>().unwrap_or(0)))
}

/// The master of an equity table, whole (it holds at most 20,000 rows), refusing what `_check_master` refuses.
pub fn read(directory: &Path, table: &Table) -> Res<(RecordBatch, Vec<Master>)> {
    let mut batches = Vec::new();
    for group in 0..table.groups {
        batches.push(table.read(directory, group)?);
    }
    let batch = match batches.len() {
        0 => return crate::error::refuse("Invalid master size"),
        1 => batches.pop().unwrap(),
        _ => return crate::error::refuse("Invalid master size"),
    };
    let n = batch.num_rows();
    let missing =
        |name: &str| crate::error::Failure::Refused(format!("Unexpected schema in {name}"));
    let u64s = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_primitive_opt::<UInt64Type>())
            .ok_or_else(|| missing(name))
    };
    let (issue, tick, kind) = (u64s("issue_id")?, u64s("tick_type")?, u64s("issue_type")?);
    let (code, name, lot, up, down) = (
        text(&batch, "raw_symbol")?,
        text(&batch, "name")?,
        text(&batch, "lot_size")?,
        text(&batch, "max_price")?,
        text(&batch, "min_price")?,
    );
    let base = batch
        .column_by_name("base_price")
        .and_then(|c| c.as_primitive_opt::<Decimal128Type>())
        .ok_or_else(|| missing("base_price"))?;
    let mut master = Vec::with_capacity(n);
    for i in 0..n {
        let lot_text = (!lot.is_null(i)).then(|| lot.value(i));
        let lot_size = match lot_text {
            Some(t) if (1..=16).contains(&t.len()) && t.bytes().all(|b| b.is_ascii_digit()) => {
                t.parse::<u64>().unwrap_or(0)
            }
            _ => return crate::error::refuse("Invalid lot size"),
        };
        let base_price10 = if base.is_null(i) {
            0
        } else {
            match tenths(base.value(i)) {
                Some(t) => t,
                None => return crate::error::refuse("Invalid decimal value"),
            }
        };
        master.push(Master {
            issue_id: issue.value(i),
            code: code.value(i).to_string(),
            tick_type: tick.value(i),
            base_price10,
            limit_up10: price_tenths((!up.is_null(i)).then(|| up.value(i)))?,
            limit_down10: price_tenths((!down.is_null(i)).then(|| down.value(i)))?,
            lot_size,
            issue_type: kind.value(i),
            name: name.value(i).to_string(),
        });
    }
    check(&master)?;
    Ok((batch, master))
}

/// `_check_master`: unique, sorted identities and valid entries.
fn check(master: &[Master]) -> Res<()> {
    require!(
        !master.is_empty() && master.len() <= 20_000,
        "Invalid master size"
    );
    let mut previous: i64 = -1;
    for m in master {
        require!(m.issue_id <= u32::MAX as u64, "Invalid unsigned integer");
        require!(
            m.issue_id as i64 > previous,
            "Master identities must be unique and sorted"
        );
        previous = m.issue_id as i64;
        require!(valid_code(&m.code), "Invalid security code");
        require!(m.name.chars().count() <= 256, "Invalid security name");
        for v in [
            m.tick_type,
            m.base_price10,
            m.limit_up10,
            m.limit_down10,
            m.lot_size,
            m.issue_type,
        ] {
            require!(v < TWO_53, "Invalid unsigned integer");
        }
    }
    Ok(())
}

/// The table the master implies, compared with the one read (`Columns._equity`): every cell, so that nothing but the
/// master can be in it.
pub fn check_canonical(batch: &RecordBatch, master: &[Master], midnight: i64) -> Res<()> {
    const BAD: &str = "Non-canonical data/equity.parquet";
    let n = batch.num_rows();
    require!(n == master.len(), BAD);
    let strings = |name: &str| text(batch, name);
    let (id, symbol, currency, increment) = (
        strings("id")?,
        strings("raw_symbol")?,
        strings("currency")?,
        strings("price_increment")?,
    );
    let (lot, isin, max_q, min_q, max_p, min_p) = (
        strings("lot_size")?,
        strings("isin")?,
        strings("max_quantity")?,
        strings("min_quantity")?,
        strings("max_price")?,
        strings("min_price")?,
    );
    let (init, maint, scheme, info, name) = (
        strings("margin_init")?,
        strings("margin_maint")?,
        strings("tick_scheme")?,
        strings("info")?,
        strings("name")?,
    );
    let precision = batch
        .column_by_name("price_precision")
        .and_then(|c| c.as_primitive_opt::<UInt8Type>());
    let times: Vec<_> = ["ts_event", "ts_init"]
        .iter()
        .map(|c| {
            batch
                .column_by_name(c)
                .and_then(|c| c.as_primitive_opt::<TimestampNanosecondType>())
        })
        .collect();
    let base = batch
        .column_by_name("base_price")
        .and_then(|c| c.as_primitive_opt::<Decimal128Type>());
    let (Some(precision), Some(Some(event)), Some(Some(init_time)), Some(base)) = (
        precision,
        times.first().copied(),
        times.get(1).copied(),
        base,
    ) else {
        return crate::error::refuse(BAD);
    };
    let optional = |array: &StringArray, i: usize, want: Option<String>| -> bool {
        match want {
            None => array.is_null(i),
            Some(w) => !array.is_null(i) && array.value(i) == w,
        }
    };
    for (i, m) in master.iter().enumerate() {
        let ok = id.value(i) == format!("{}{}", m.code, crate::schema::VENUE_SUFFIX)
            && symbol.value(i) == m.code
            && currency.value(i) == "JPY"
            && precision.value(i) == 1
            && increment.value(i) == "0.1"
            && optional(lot, i, Some(m.lot_size.to_string()))
            && [isin, max_q, min_q, scheme, info]
                .iter()
                .all(|a| a.is_null(i))
            && optional(max_p, i, price_text(m.limit_up10))
            && optional(min_p, i, price_text(m.limit_down10))
            && init.value(i) == "0"
            && maint.value(i) == "0"
            && event.value(i) == midnight
            && init_time.value(i) == midnight
            && name.value(i) == m.name
            && (if m.base_price10 == 0 {
                base.is_null(i)
            } else {
                !base.is_null(i) && tenths(base.value(i)) == Some(m.base_price10)
            });
        require!(ok, BAD);
    }
    Ok(())
}
